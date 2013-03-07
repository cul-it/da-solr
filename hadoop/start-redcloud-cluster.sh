#!/bin/bash

EUCA_DIR=/lib-dev/eucalyptus
SLAVES=1
#SLAVE_SIZE=m1.large
SLAVE_SIZE=c1.medium
EUCA_KEY_FILE=$EUCA_DIR/mykey.private
EUCA_KEY_NAME=mykey

HADOOP_HOME=$EUCA_DIR/hadoop

cd $EUCA_DIR

# source the bash script at eucarc
# This will allow you to work with the RedCloud eucalyptus VM system.
source $EUCA_DIR/eucarc

# check to make sure that there are no VMs already in existence
# if [ $(euca-describe-instances | grep ".*" ) ] 
# then
#   echo "There are VMs running, this script must only be used when there are no VMs to start with"
#   exit 1
# fi

# Start a small VM for the master and  medium ones for the slaves
echo "starting vm nodes, this may take a while"
$HADOOP_HOME/blocking-start-instances.py "(1,'m1.small'),($SLAVES,'$SLAVE_SIZE')"

# Create a files with addresses of vm nodes.  
# These files are addresses.txt, hosts, salves.txt, 
# master.txt, hadoop/conf/masters hadoop/conf/slaves files
$HADOOP_HOME/make-addresses-files.py

# Setup SSH args to use for pdsh.
# We need to specify the private key to connect to the VMs.
export PDSH_SSH_ARGS_APPEND=" -i $EUCA_KEY_FILE \
-o StrictHostKeychecking=no -o UserKnownHostsFile=/dev/null "

#install pdcp nodes if needed
pdsh -l root -w^addresses.txt "which pdsh || apt-get -y install pdsh"

# copy host file to all nodes
pdcp -l root -w^addresses.txt hosts /etc/hosts

# set hostname on all hosts by sourceing file created by make-addresses-files.py
source hostnames.sh

# Each VM comes with a block device that is unformatted and not mounted. 
# There is a script that Brian Caruso wrote on the disk image that will 
# format the device and mount it at /mnt.
# Only run prepareStorage.sh if there is no mnt in mount's output.
pdsh -l root -w^addresses.txt "mount | grep mnt || ./prepareStorage.sh"

# Make a new DSA key for hadoop to communicate across the 
# nodes of the cluster. 
if [ -e ./hadoop_dsa ] 
then
    mv ./hadoop_dsa ./hadoop_dsa$(date "+%s")
fi
ssh-keygen -t dsa -P '' -f ./hadoop_dsa
pdsh -l root -w^addresses.txt mkdir  /home/hadoop/.ssh
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
pdsh -l root -w^./addresses.txt chown hadoop:hadoop -R \
 /home/hadoop /mnt/hadoop /mnt/mapredLocal

# Format hadoop fs
ssh -o StrictHostKeychecking=no -o UserKnownHostsFile=/dev/null \
 -i ./hadoop_dsa hadoop@$(cat master.txt) "bash -i " <<EOF
pwd
cd
pwd
yes Y | bin/hadoop namenode -format
sleep 3
bin/hadooop namenode &
sleep 3 
bin/start-dfs.sh
sleep 3
bin/start-mapred.sh
EOF