#!/bin/sh
### BEGIN REDHAT INFO
# chkconfig: 2345 99 20
# description: The Rexster server. See http://rexster.tinkerpop.com
### END REDHAT INFO
### BEGIN INIT INFO
# Provides:          rexster
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

# Init script for Rexster so it automatically starts/stops with the machine.
#
# To install:
# 1)  Add a symlink to this file in /etc/init.d/ under the name you'd like to see the service
#     For example, to name the service "rexster": ln -s /usr/local/packages/dynamodb-titan054-storage-backend-1.0.0-hadoop2/bin/rexster-service.sh /etc/init.d/rexster
# 2a) If you're running RH: chkconfig --add rexster
# 2b) If you're running Ubuntu: update-rc.d rexster defaults
#
# You have to SET the Rexster installation directory here:
REXSTER_DIR="/usr/local/packages/dynamodb-titan054-storage-backend-1.0.1-hadoop2"
REXSTER_LOG_DIR="/var/log/rexster"
# Specify the user to run Rexster as:
REXSTER_USER="ec2-user"
# JAVA_OPTIONS only gets used on start
JAVA_OPTIONS="-server -Xms128m -Xmx512m -Dtitan.logdir=$REXSTER_LOG_DIR"


usage() {
    echo "Usage: `basename $0`: <start|stop|status>"
    exit 1
}

start() {
    status
    if [ $PID -gt 0 ]
    then
        echo "Rexster server has already been started. PID: $PID"
        return $PID
    fi
    export JAVA_OPTIONS
    echo "Starting Rexster server..."
    su -c "cd \"$REXSTER_DIR\"; /usr/bin/nohup ./bin/rexster.sh --start -c ${REXSTER_DIR}/conf/rexster.xml 1>$REXSTER_LOG_DIR/service.log 2>$REXSTER_LOG_DIR/service.err &" $REXSTER_USER
}

stop() {
    status
    if [ $PID -eq 0 ]
    then
        echo "Rexster server has already been stopped."
        return 0
    fi
    echo "Stopping Rexster server..."
    su -c "cd \"$REXSTER_DIR\"; /usr/bin/nohup ./bin/rexster.sh -x 1>$REXSTER_LOG_DIR/service-stop.log 2>$REXSTER_LOG_DIR/service-stop.err &" $REXSTER_USER
}

status() {
    PID=`ps -ef | grep $REXSTER_USER | grep java | grep Application | grep -v grep | awk '{print $2}'`
    if [ "x$PID" = "x" ]
    then
        PID=0
    fi

    # if PID is greater than 0 then Rexster is running, else it is not
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
        echo "Rexster server is running with PID: $PID"
    else
        echo "Rexster server is NOT running"
    fi
    exit $PID
fi

usage
