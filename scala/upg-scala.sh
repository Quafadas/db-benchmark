#!/bin/bash
set -e

echo 'upgrading scala-cli...'

# Update scala-cli
curl -sSLf https://scala-cli.virtuslab.org/get | sh

./scala/ver-scala.sh
