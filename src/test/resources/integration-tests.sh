#!/bin/bash

mvn install -P integration-tests -Dgroups="${CATEGORY}" -Dinclude.category="**/*.java"