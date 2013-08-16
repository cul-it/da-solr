#!/bin/bash

# format and mount the extra drive space provided by RedCloud
mkfs -t ext3 /dev/vda2
tune2fs -m 0 /dev/vda2
mount /dev/vda2 /mnt
