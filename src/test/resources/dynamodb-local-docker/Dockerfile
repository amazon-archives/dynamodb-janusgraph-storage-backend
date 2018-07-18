#
# Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Portions copyright 2017 JanusGraph authors
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#  http://aws.amazon.com/apache2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
#
FROM amazonlinux

RUN yum update -y -q -e 0 && yum upgrade -y -q -e 0 \
    && yum install -y -q java-1.8.0-openjdk sqlite3 libsqlite3-dev wget tar gzip \
    && mkdir -p /var/dynamodblocal
WORKDIR /var/dynamodblocal
RUN wget https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.tar.gz -q -O - | tar -xz
EXPOSE 8000
ENTRYPOINT ["java", "-jar", "DynamoDBLocal.jar", "-inMemory"]
