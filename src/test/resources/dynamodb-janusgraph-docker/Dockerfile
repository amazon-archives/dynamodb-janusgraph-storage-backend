#
# Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
FROM openjdk:8

ARG server_zip
ADD ${server_zip} /var

RUN chmod a+x /usr/bin/wait-for-dynamodb-local.sh && \
    apt-get update -y && apt-get install -y zip && \
    server_base=`basename ${server_zip} .zip` && \
    unzip -q /var/${server_base}.zip -d /var && \
    mv /var/${server_base} /var/jg

WORKDIR /var/jg
