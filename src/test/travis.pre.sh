#!/bin/bash

set -e

# We need tarantool 2.* for jdbc/sql.
curl http://download.tarantool.org/tarantool/$TNT_REPO/gpgkey | sudo apt-key add -
release=`lsb_release -c -s`

sudo rm -f /etc/apt/sources.list.d/*tarantool*.list
sudo tee /etc/apt/sources.list.d/tarantool_${TNT_REPO}.list <<- EOF
deb http://download.tarantool.org/tarantool/$TNT_REPO/ubuntu/ $release main
deb-src http://download.tarantool.org/tarantool/$TNT_REPO/ubuntu/ $release main
EOF

sudo apt-get update
sudo apt-get -y install tarantool tarantool-common

sudo tarantoolctl stop example
