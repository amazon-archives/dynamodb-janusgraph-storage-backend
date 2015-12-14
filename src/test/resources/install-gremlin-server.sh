#!/bin/bash

#
# Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
# http://aws.amazon.com/apache2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
#

#collect the prereqs and build the plugin
mvn clean
mvn install

export ARTIFACT_NAME="dynamodb-titan100-storage-backend"
export TITAN_DYNAMODB_HOME=${PWD}
export TITAN_DYNAMODB_TARGET=${TITAN_DYNAMODB_HOME}/target
export TITAN_VERSION="1.0.0"
export DYNAMODB_PLUGIN_VERSION="1.0.0"
export TITAN_VANILLA_SERVER_DIRNAME=titan-${TITAN_VERSION}-hadoop1
export TITAN_VANILLA_SERVER_ZIP=${TITAN_VANILLA_SERVER_DIRNAME}.zip
export TITAN_DYNAMODB_SERVER_DIRNAME=${ARTIFACT_NAME}-${DYNAMODB_PLUGIN_VERSION}-hadoop1
export TITAN_SERVER_HOME=${TITAN_DYNAMODB_HOME}/server/${TITAN_DYNAMODB_SERVER_DIRNAME}
export TITAN_DYNAMODB_SERVER_ZIP=${TITAN_DYNAMODB_SERVER_DIRNAME}.zip
export TITAN_SERVER_CONF=${TITAN_SERVER_HOME}/conf
export TITAN_GREMLIN_SERVER_CONF=${TITAN_SERVER_CONF}/gremlin-server
export TITAN_SERVER_BIN=${TITAN_SERVER_HOME}/bin
export TITAN_DYNAMODB_EXT_DIR=${TITAN_SERVER_HOME}/ext/${ARTIFACT_NAME}
export TITAN_SERVER_YAML=${TITAN_GREMLIN_SERVER_CONF}/gremlin-server.yaml
export TITAN_SERVER_LOCAL_YAML=${TITAN_GREMLIN_SERVER_CONF}/gremlin-server-local.yaml
export TITAN_SERVER_DYNAMODB_PROPERTIES=${TITAN_GREMLIN_SERVER_CONF}/dynamodb.properties
export TITAN_SERVER_DYNAMODB_LOCAL_PROPERTIES=${TITAN_GREMLIN_SERVER_CONF}/dynamodb-local.properties
export TITAN_DYNAMODB_TEST_RESOURCES=${TITAN_DYNAMODB_HOME}/src/test/resources
export TITAN_SERVER_SERVICE_SH=${TITAN_SERVER_BIN}/gremlin-server-service.sh

#download the server products
mkdir -p ${TITAN_DYNAMODB_HOME}/server
pushd ${TITAN_DYNAMODB_HOME}/server
curl -s -O http://s3.thinkaurelius.com/downloads/titan/${TITAN_VANILLA_SERVER_ZIP}

#unpack
unzip -qq ${TITAN_VANILLA_SERVER_ZIP} -d ${TITAN_DYNAMODB_HOME}/server
mv ${TITAN_VANILLA_SERVER_DIRNAME} ${TITAN_DYNAMODB_SERVER_DIRNAME}
rm ${TITAN_VANILLA_SERVER_ZIP}

#load extra dependencies
mkdir -p ${TITAN_DYNAMODB_EXT_DIR}
cp ${TITAN_DYNAMODB_TARGET}/${ARTIFACT_NAME}-${DYNAMODB_PLUGIN_VERSION}.jar ${TITAN_DYNAMODB_EXT_DIR}
cp -R ${TITAN_DYNAMODB_TARGET}/dependencies/*.* ${TITAN_DYNAMODB_EXT_DIR}
#fix bad dependencies
mkdir ${TITAN_SERVER_HOME}/badlibs
pushd ${TITAN_SERVER_HOME}/lib
mv joda-time-1.6.2.jar ${TITAN_SERVER_HOME}/badlibs
mv jackson-annotations-2.3.0.jar ${TITAN_SERVER_HOME}/badlibs
mv jackson-core-2.3.0.jar ${TITAN_SERVER_HOME}/badlibs
mv jackson-databind-2.3.0.jar ${TITAN_SERVER_HOME}/badlibs
mv jackson-datatype-json-org-2.3.0.jar ${TITAN_SERVER_HOME}/badlibs
popd

#copy over dynamodb configuration
cp ${TITAN_DYNAMODB_TEST_RESOURCES}/gremlin-server.yaml ${TITAN_SERVER_YAML}
cp ${TITAN_DYNAMODB_TEST_RESOURCES}/gremlin-server-local.yaml ${TITAN_SERVER_LOCAL_YAML}
cp ${TITAN_DYNAMODB_TEST_RESOURCES}/dynamodb.properties ${TITAN_SERVER_DYNAMODB_PROPERTIES}
cp ${TITAN_DYNAMODB_TEST_RESOURCES}/dynamodb-local.properties ${TITAN_SERVER_DYNAMODB_LOCAL_PROPERTIES}
cp ${TITAN_DYNAMODB_TEST_RESOURCES}/gremlin-server-service.sh ${TITAN_SERVER_SERVICE_SH}

#show how to call the startup script
echo ""
echo "Change directories to the server root:"
echo "cd server/${TITAN_DYNAMODB_SERVER_DIRNAME}"
echo ""
echo "Start Gremlin Server against us-east-1 with the following command (uses the default credential provider chain):"
echo "bin/gremlin-server.sh ${TITAN_SERVER_YAML}"
echo ""
echo "Start Gremlin Server against DynamoDB Local with the following command (remember to start DynamoDB Local first with mvn test -Pstart-dynamodb-local):"
echo "bin/gremlin-server.sh ${TITAN_SERVER_LOCAL_YAML}"
echo ""
echo "Connect to Gremlin Server using the Gremlin console:"
echo "bin/gremlin.sh"
echo ""
echo "Connect to the graph on Gremlin Server:"
echo ":remote connect tinkerpop.server conf/remote.yaml"

#repackage the server
zip -rq ${TITAN_DYNAMODB_SERVER_ZIP} ${TITAN_DYNAMODB_SERVER_DIRNAME}
popd
