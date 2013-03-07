#!/bin/bash

EUCA_DIR=/lib-dir/eucalyptus
SLAVES=4
EUCA_KEY_FILE=$EUCA_DIR/mykey.private
EUCA_KEY_NAME=mykey

HADOOP_HOME=$EUCA_DIR/hadoop

cd $EUCA_DIR

# source the bash script at eucarc
# This will allow you to work with the RedCloud eucalyptus VM system.
source $EUCA_DIR/eucarc

# check to make sure that there are no VMs already in existence
if [ $(euca-describe-instances | grep ".*" ) ] 
then
  echo "There are already vms running, this script must only run when there are no vms to start with"
  exit(1)
fi

# Start a small VM for the master and  medium ones for the slaves
$HADOOP_HOME/blocking-start-instances.py "(1,'m1.small'),($SLAVES,'m1.large)"

# Create a files with addresses of vm nodes.  
# These files are addresses.txt, hosts, salves.txt, 
# master.txt, hadoop/conf/masters hadoop/conf/slaves files
$HADOOP_HOME/make-addresses-files.py

# Setup SSH args to use for pdsh.
# We need to specify the private key to connect to the VMs.
PDSH_SSH_ARGS_APPEND=" -i $EUCA_KEY_FILE "

# copy host file to all nodes
pdcp -l root -w^addresses.txt hosts /etc/hosts

# Each VM comes with a block device that is unformatted and not mounted. 
# There is a script that Brian Caruso wrote on the disk image that will 
# format the device and mount it at /mnt.
pdsh -l root -w^addresses.txt ./prepareStorage.sh

# Make a new DSA key for hadoop to communicate across the 
# nodes of the cluster. 
ssh-keygen -t dsa -P '' -f ./hadoop_dsa
pdcp -l root -w^./addresses.txt hadoop_dsa.pub /home/hadoop/.ssh/authorized_keys
pdcp -l root -w^./addresses.txt hadoop_dsa  /home/hadoop/.ssh/id_dsa

# Copy the hadoop dir to all servers
pdcp -r -l root -w^./addresses.txt $HADOOP_HOME/* /home/hadoop/
pdsh -l root -w^./addresses.txt chown -R hadoop:hadoop /home/hadoop

# Create the directories mentioned in the hadoop/conf files
pdsh -l root -w^./addresses.txt mkdir -p /mnt/hadoop/tmp
pdsh -l root -w^./addresses.txt mkdir -p /mnt/hadoop/mapred/system
pdsh -l root -w^addresses.txt mkdir /mnt/mapredLocal
pdsh -l root -w^addresses.txt mkdir /home/hadoop/mapredLocal

#set ownership on these hadoop files 
pdsh -l root -w^./addresses.txt chown hadoop:hadoop -R /home/hadoop /mnt/hadoop /mnt/mapredLocal
