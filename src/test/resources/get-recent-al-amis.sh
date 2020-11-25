#!/bin/bash
#
# Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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


LATER_THAN_DATE=$1

aws ec2 describe-regions --query 'Regions[*].[RegionName]' | sed -e '/\[/d' -e '/\]/d' -e "s/^[ \t]*//" | sed 's/\"//g' | sort | while read region; do
  echo "    ${region}:"
  aws ec2 describe-images \
  --region ${region} \
  --owners amazon \
  --filters "Name=root-device-type,Values=ebs" "Name=name,Values=amzn2-ami*" \
  --query 'Images[? CreationDate > `'${LATER_THAN_DATE}'` && !contains(Name, `minimal`) && !contains(Name, `dotnetcore`) && !contains(Name, `nat`)].[ImageId, ImageLocation]' |\
    grep "\"" |\
    sed -e 's/^[ 	]*\(.*\)[ 	]*$/\1/' -e '/\"$/s/$/%/' |\
    tr "\n" "@" |\
    tr "%" "\n" |\
    sed -e 's/[ ]*@//g' \
        -e "/^[ \t]*$/d" \
        -e 's/\"//g' \
        -e 's/\(.*\),\(.*\)/\2,\1/' \
        -e '/pv/s/\(.*\),\(.*\)/PV64:\ \2/' \
        -e '/x86_64-gp2/s/\(.*\),\(.*\)/HVMG2:\ \2/' \
        -e '/x86_64-ebs/s/\(.*\),\(.*\)/HVM64:\ \2/' \
        -e 's/^/\ \ \ \ \ \ /' | sort
done
