#!/bin/bash
set -e

# install scala-cli
curl -sSLf https://scala-cli.virtuslab.org/get | sh

# add to path
echo 'export PATH=$PATH:$HOME/.local/share/scala-cli' >> path.env

source path.env

# verify installation
scala-cli --version --dep io.github.quafadas::scautable::0.0.33

./scala/ver-scala.sh
