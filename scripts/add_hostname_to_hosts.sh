#!/usr/bin/env bash

#
# /* Copyright (C) Vast Data Ltd. */
#

[[ -z $1 ]] && echo "IP must be provided" && exit 1

set -x

HOSTNAME=$(hostname)
LINE="$1    $HOSTNAME"
grep "$LINE" "/etc/hosts" && exit 0
echo "$LINE" | sudo tee -a /etc/hosts
