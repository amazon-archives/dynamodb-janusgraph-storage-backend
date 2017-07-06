#!/bin/bash
wget https://github.com/amcp/dynamodb-import-export-tool/archive/createTable.zip
unzip -q createTable.zip
rm createTable.zip
pushd dynamodb-import-export-tool-createTable
mvn install
pushd target

export ORIGINAL_TABLE_ENDPOINT=$1
export ORIGINAL_TABLE_REGION=$2
export ORIGINAL_TABLE_PREFIX=$3
export NEW_TABLE_ENDPOINT=$4
export NEW_TABLE_REGION=$5
export NEW_TABLE_PREFIX=$6

declare -a tableSuffixes=("edgestore" "graphindex" "system_properties" "systemlog" "titan_ids" "txlog")

for tableSuffix in "${tableSuffixes[@]}"; do
    ORIGINAL_TABLE_NAME="${ORIGINAL_TABLE_PREFIX}_${tableSuffix}"
    NEW_TABLE_NAME="${NEW_TABLE_PREFIX}_${tableSuffix}"
    # prepare graph for cross-region replication
    # https://github.com/awslabs/dynamodb-cross-region-library#step-1-optional-table-copy-bootstrapping-existing-data

    # enable stream in source tables
    aws dynamodb update-table \
        --endpoint ${ORIGINAL_TABLE_ENDPOINT} \
        --table-name ${ORIGINAL_TABLE_NAME} \
        --stream-specification "StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES"

    # bootstrap destination tables
    java -jar dynamodb-import-export-tool-1.1.0.jar \
        --sourceSigningRegion ${ORIGINAL_TABLE_REGION} \
        --sourceEndpoint ${ORIGINAL_TABLE_ENDPOINT} \
        --sourceTable ${ORIGINAL_TABLE_NAME} \
        --destinationSigningRegion ${NEW_TABLE_REGION} \
        --destinationEndpoint ${NEW_TABLE_ENDPOINT} \
        --destinationTable ${NEW_TABLE_NAME} \
        --readThroughputRatio 0.5 \
        --writeThroughputRatio 1 \
        --createDestination
    
    echo "copied table ${ORIGINAL_TABLE_NAME} at ${ORIGINAL_TABLE_ENDPOINT} to ${NEW_TABLE_NAME} at ${NEW_TABLE_ENDPOINT}"
done

popd
popd

# clean up
rm -rf dynamodb-import-export-tool-createTable
