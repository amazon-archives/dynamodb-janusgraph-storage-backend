#!/bin/bash

LATER_THAN_DATE=$1

aws ec2 describe-regions --query 'Regions[*].[RegionName]' | sed -e '/\[/d' -e '/\]/d' -e "s/^[ \t]*//" | sed 's/\"//g' | while read region; do
  echo "    ${region}:"
  aws ec2 describe-images \
  --region ${region} \
  --owners amazon \
  --filters "Name=root-device-type,Values=ebs" "Name=name,Values=amzn-ami-*" \
  --query 'Images[? CreationDate > `'${LATER_THAN_DATE}'` && !contains(Name, `minimal`) && !contains(Name, `nat`)].[ImageId, ImageLocation]' |\
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
        -e 's/^/\ \ \ \ \ \ /'
done
