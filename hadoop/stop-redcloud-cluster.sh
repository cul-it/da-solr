#!/bin/bash

# this script terminates all running VMs from euca

EUCA_DIR=/lib-dev/eucalyptus

cd $EUCA_DIR
source eucarc
euca-describe-instances | grep INSTANCE | cut -f 2 | xargs euca-terminate-instances

cd -
