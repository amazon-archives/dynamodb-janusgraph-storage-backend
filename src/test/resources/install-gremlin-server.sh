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
mvn clean install -DskipTests=true

export ARTIFACT_NAME="dynamodb-janusgraph010-storage-backend"
export JANUSGRAPH_DYNAMODB_HOME=${PWD}
export JANUSGRAPH_DYNAMODB_TARGET=${JANUSGRAPH_DYNAMODB_HOME}/target
export JANUSGRAPH_VERSION="0.1.0-SNAPSHOT"
#Extract the DYNAMODB version from the pom.
export DYNAMODB_PLUGIN_VERSION=`mvn -q -Dexec.executable="echo" -Dexec.args='${project.version}' --non-recursive org.codehaus.mojo:exec-maven-plugin:1.3.1:exec`
export JANUSGRAPH_VANILLA_SERVER_DIRNAME=janusgraph-${JANUSGRAPH_VERSION}-hadoop2
export JANUSGRAPH_VANILLA_SERVER_ZIP=${JANUSGRAPH_VANILLA_SERVER_DIRNAME}.zip
export JANUSGRAPH_DYNAMODB_SERVER_DIRNAME=${ARTIFACT_NAME}-${DYNAMODB_PLUGIN_VERSION}-hadoop1
export JANUSGRAPH_SERVER_HOME=${JANUSGRAPH_DYNAMODB_HOME}/server/${JANUSGRAPH_DYNAMODB_SERVER_DIRNAME}
export JANUSGRAPH_DYNAMODB_SERVER_ZIP=${JANUSGRAPH_DYNAMODB_SERVER_DIRNAME}.zip
export JANUSGRAPH_SERVER_CONF=${JANUSGRAPH_SERVER_HOME}/conf
export JANUSGRAPH_GREMLIN_SERVER_CONF=${JANUSGRAPH_SERVER_CONF}/gremlin-server
export JANUSGRAPH_SERVER_BIN=${JANUSGRAPH_SERVER_HOME}/bin
export JANUSGRAPH_DYNAMODB_EXT_DIR=${JANUSGRAPH_SERVER_HOME}/ext/${ARTIFACT_NAME}
export JANUSGRAPH_SERVER_YAML=${JANUSGRAPH_GREMLIN_SERVER_CONF}/gremlin-server.yaml
export JANUSGRAPH_SERVER_LOCAL_YAML=${JANUSGRAPH_GREMLIN_SERVER_CONF}/gremlin-server-local.yaml
export JANUSGRAPH_SERVER_DYNAMODB_PROPERTIES=${JANUSGRAPH_GREMLIN_SERVER_CONF}/dynamodb.properties
export JANUSGRAPH_SERVER_DYNAMODB_LOCAL_PROPERTIES=${JANUSGRAPH_GREMLIN_SERVER_CONF}/dynamodb-local.properties
export JANUSGRAPH_DYNAMODB_TEST_RESOURCES=${JANUSGRAPH_DYNAMODB_HOME}/src/test/resources
export JANUSGRAPH_SERVER_SERVICE_SH=${JANUSGRAPH_SERVER_BIN}/gremlin-server-service.sh

#download the server products
mkdir -p ${JANUSGRAPH_DYNAMODB_HOME}/server
pushd ${JANUSGRAPH_DYNAMODB_HOME}/server
#curl -s -O http://s3.thinkaurelius.com/downloads/titan/${JANUSGRAPH_VANILLA_SERVER_ZIP}
#build server product until a release happens
git clone https://github.com/JanusGraph/janusgraph.git
pushd janusgraph
mvn clean package -Pjanusgraph-release -Dgpg.skip=true -DskipTests=true
mv janusgraph-dist/janusgraph-dist-hadoop-2/target/${JANUSGRAPH_VANILLA_SERVER_ZIP} ..
popd

#unpack
unzip -qq ${JANUSGRAPH_VANILLA_SERVER_ZIP} -d ${JANUSGRAPH_DYNAMODB_HOME}/server
mv ${JANUSGRAPH_VANILLA_SERVER_DIRNAME} ${JANUSGRAPH_DYNAMODB_SERVER_DIRNAME}
rm ${JANUSGRAPH_VANILLA_SERVER_ZIP}

#load extra dependencies
mkdir -p ${JANUSGRAPH_DYNAMODB_EXT_DIR}
cp ${JANUSGRAPH_DYNAMODB_TARGET}/${ARTIFACT_NAME}-${DYNAMODB_PLUGIN_VERSION}.jar ${JANUSGRAPH_DYNAMODB_EXT_DIR}
cp -R ${JANUSGRAPH_DYNAMODB_TARGET}/dependencies/*.* ${JANUSGRAPH_DYNAMODB_EXT_DIR}
#fix bad dependencies
mkdir ${JANUSGRAPH_SERVER_HOME}/badlibs
pushd ${JANUSGRAPH_SERVER_HOME}/lib
mv joda-time-1.6.2.jar ${JANUSGRAPH_SERVER_HOME}/badlibs
mv jackson-annotations-2.3.0.jar ${JANUSGRAPH_SERVER_HOME}/badlibs
mv jackson-core-2.3.0.jar ${JANUSGRAPH_SERVER_HOME}/badlibs
mv jackson-databind-2.3.0.jar ${JANUSGRAPH_SERVER_HOME}/badlibs
mv jackson-datatype-json-org-2.3.0.jar ${JANUSGRAPH_SERVER_HOME}/badlibs
popd

#copy over dynamodb configuration
cp ${JANUSGRAPH_DYNAMODB_TEST_RESOURCES}/gremlin-server.yaml ${JANUSGRAPH_SERVER_YAML}
cp ${JANUSGRAPH_DYNAMODB_TEST_RESOURCES}/gremlin-server-local.yaml ${JANUSGRAPH_SERVER_LOCAL_YAML}
cp ${JANUSGRAPH_DYNAMODB_TEST_RESOURCES}/dynamodb.properties ${JANUSGRAPH_SERVER_DYNAMODB_PROPERTIES}
cp ${JANUSGRAPH_DYNAMODB_TEST_RESOURCES}/dynamodb-local.properties ${JANUSGRAPH_SERVER_DYNAMODB_LOCAL_PROPERTIES}
cp ${JANUSGRAPH_DYNAMODB_TEST_RESOURCES}/gremlin-server-service.sh ${JANUSGRAPH_SERVER_SERVICE_SH}

#show how to call the startup script
echo ""
echo "Change directories to the server root:"
echo "cd server/${JANUSGRAPH_DYNAMODB_SERVER_DIRNAME}"
echo ""
echo "Start Gremlin Server against us-east-1 with the following command (uses the default credential provider chain):"
echo "bin/gremlin-server.sh ${JANUSGRAPH_SERVER_YAML}"
echo ""
echo "Start Gremlin Server against DynamoDB Local with the following command (remember to start DynamoDB Local first with mvn test -Pstart-dynamodb-local):"
echo "bin/gremlin-server.sh ${JANUSGRAPH_SERVER_LOCAL_YAML}"
echo ""
echo "Connect to Gremlin Server using the Gremlin console:"
echo "bin/gremlin.sh"
echo ""
echo "Connect to the graph on Gremlin Server:"
echo ":remote connect tinkerpop.server conf/remote.yaml"

#repackage the server
zip -rq ${JANUSGRAPH_DYNAMODB_SERVER_ZIP} ${JANUSGRAPH_DYNAMODB_SERVER_DIRNAME}
popd
