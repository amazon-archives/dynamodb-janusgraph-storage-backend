#!/bin/sh
### BEGIN REDHAT INFO
# chkconfig: 2345 99 20
# description: The Gremlin Server. See http://tinkerpop.incubator.apache.org/docs/3.0.1-incubating/#gremlin-server
### END REDHAT INFO
### BEGIN INIT INFO
# Provides:          gremlin-server
# Required-Start:
# Required-Stop:
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
### END INIT INFO

#!/bin/bash

#
# Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
# This file was adapted from the rexster-service.sh script here:
# https://github.com/tinkerpop/rexster/blob/2.5.0/rexster-server/src/main/bin/rexster-service.sh
#
# Original license:
#
# Copyright (c) 2009-Infinity, TinkerPop [http://tinkerpop.com]
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the TinkerPop nor the
#       names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL TINKERPOP BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

# Init script for Gremlin Server so it automatically starts/stops with the machine.
#
# To install:
# 1)  Add a symlink to this file in /etc/init.d/ under the name you'd like to see the service
#     For example, to name the service "gremlin-server": ln -s /usr/local/packages/dynamodb-titan100-storage-backend-1.0.0-hadoop1/bin/gremlin-server-service.sh /etc/init.d/gremlin-server
# 2a) If you're running RH: chkconfig --add gremlin-server
# 2b) If you're running Ubuntu: update-rc.d gremlin-server defaults
#
# You have to SET the Gremlin Server installation directory here:
GREMLIN_SERVER_DIR="/usr/local/packages/dynamodb-titan100-storage-backend-1.0.0-hadoop1"
GREMLIN_SERVER_LOG_DIR="/var/log/gremlin-server"
# Specify the user to run Gremlin Server as:
GREMLIN_SERVER_USER="ec2-user"
# JAVA_OPTIONS only gets used on start
JAVA_OPTIONS="-server -Xms128m -Xmx512m -Dtitan.logdir=$GREMLIN_SERVER_LOG_DIR"


usage() {
    echo "Usage: `basename $0`: <start|stop|status>"
    exit 1
}

start() {
    status
    if [ $PID -gt 0 ]
    then
        echo "Gremlin Server has already been started. PID: $PID"
        return $PID
    fi
    export JAVA_OPTIONS
    echo "Starting Gremlin Server..."
    su -c "cd \"$GREMLIN_SERVER_DIR\"; /usr/bin/nohup ./bin/gremlin-server.sh ${GREMLIN_SERVER_DIR}/conf/gremlin-server/gremlin-server.yaml 1>$GREMLIN_SERVER_LOG_DIR/service.log 2>$GREMLIN_SERVER_LOG_DIR/service.err &" $GREMLIN_SERVER_USER
}

stop() {
    status
    if [ $PID -eq 0 ]
    then
        echo "Gremlin Server has already been stopped."
        return 0
    fi
    echo "Stopping Gremlin Server..."
    su -c "kill -9 ${PID}" $GREMLIN_SERVER_USER
}

status() {
    PID=`ps -ef | grep $GREMLIN_SERVER_USER | grep java | grep GremlinServer | grep -v grep | awk '{print $2}'`
    if [ "x$PID" = "x" ]
    then
        PID=0
    fi

    # if PID is greater than 0 then Gremlin Server is running, else it is not
    return $PID
}

if [ "x$1" = "xstart" ]
then
    start
    exit 0
fi

if [ "x$1" = "xstop" ]
then
    stop
    exit 0
fi

if [ "x$1" = "xstatus" ]
then
    status
    if [ $PID -gt 0 ]
    then
        echo "Gremlin Server is running with PID: $PID"
    else
        echo "Gremlin Server is NOT running"
    fi
    exit $PID
fi

usage
