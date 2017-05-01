#!/bin/bash

cat $1 | while read instance_type; do
  echo "    ${instance_type}:"
  if [[ $instance_type == "g2"* ]]; then
    echo "      Arch: HVMG2"
  else
    echo "      Arch: HVM64"
  fi
done
