#!/bin/bash
set -e

# Get scala-cli version
scala-cli version 2>&1 | grep "Scala CLI version" | awk '{print $4}' > scala/VERSION
echo "" > scala/REVISION
