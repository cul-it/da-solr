#!/bin/bash

EUCA_DIR=/lib-dev/eucalyptus

#number of slaves to run
SLAVES=4

#Size of the VM to run as slaves 
# valid options c1.medium m1.large  m1.xlarge c1.xlarge
# slaves must be larger than m1.small
SLAVE_SIZE=c1.medium

#file with the private key for $EUCA_KEY_NAME
EUCA_KEY_FILE=$EUCA_DIR/bdc34-euca3-key.pem

#name of the private key to put on the VMs for root ssh access
EUCA_KEY_NAME=bdc34-euca3-key

# Disk image to use for all the VMs
DISK_EMI=emi-CC163D50

# security group to run all the VMs in
EUCA_SECURITY_GROUP=da-hadoop

HADOOP_HOME=$EUCA_DIR/hadoop

cd $EUCA_DIR

# source the bash script at eucarc
# This will allow you to work with the RedCloud eucalyptus VM system.
source $EUCA_DIR/eucarc

SKIP_VM_STARTUP=0

while test $# != 0
do
    case "$1" in
    --skip-startup) SKIP_VM_STARTUP=1 ;;
#    -A) all_into_one=t
#        unpack_unreachable=--unpack-unreachable ;;
#    -d) remove_redundant=t ;;
    --slaves) SLAVES=$2; shift ;;
    --) shift; break;;
    *)  echo "start-redcloud-cluster.sh [--skip-startup] [--slaves #]" ; exit ;;
    esac
    shift
done

# Start a small VM for the master and larger ones for the slaves
if [[ $SKIP_VM_STARTUP  = 1 ]]
then 
  echo 'Skip VM startup'
else
  echo "Starting vm nodes, this may take a while, maybe even 5 minutes."
  $HADOOP_HOME/blocking-start-instances.py "(1,'m1.small'),($SLAVES,'$SLAVE_SIZE')" \
      $DISK_EMI $EUCA_KEY_NAME $EUCA_SECURITY_GROUP
fi

# Create a files with addresses of vm nodes.  
# These files are addresses.txt, hosts, salves.txt, 
# master.txt, hadoop/conf/masters hadoop/conf/slaves files
$HADOOP_HOME/make-addresses-files.py

# Setup SSH args to use for pdsh.
# We need to specify the private key to connect to the VMs.
export QUIET_SSH_OPTIONS=" -o StrictHostKeychecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR " 
export PDSH_SSH_ARGS_APPEND=" -i $EUCA_KEY_FILE $QUIET_SSH_OPTIONS "

#install pdcp nodes if needed
pdsh -l root -w^addresses.txt "which pdsh || apt-get -y install pdsh"

#install java 7 if needed 
pdsh -l root -w^addresses.txt "apt-get -y install openjdk-7-jdk "

# copy host file to all nodes
pdcp -l root -w^addresses.txt privateHosts /etc/hosts

# set hostname on all hosts by sourceing file created by make-addresses-files.py
source hostnames.sh

# Each VM comes with a block device that is unformatted and not mounted. 
# There is a script that Brian Caruso wrote on the disk image that will 
# format the device and mount it at /mnt.
# Only run prepareStorage.sh if there is no mnt in mount's output.
pdsh -l root -w^addresses.txt "mount | grep mnt || source ./prepareStorage.sh"

# Make a new DSA key for hadoop to communicate across the 
# nodes of the cluster. 
if [ -e ./hadoop_dsa ] 
then
    mv ./hadoop_dsa ./hadoop_dsa$(date "+%s")
fi
ssh-keygen -q -t dsa -P '' -f ./hadoop_dsa

pdsh -l root -w^addresses.txt mkdir -p /home/hadoop/.ssh
pdcp -l root -w^./addresses.txt hadoop_dsa.pub /home/hadoop/.ssh/authorized_keys
pdcp -l root -w^./addresses.txt hadoop_dsa  /home/hadoop/.ssh/id_dsa
pdcp -l root -w^./addresses.txt $HADOOP_HOME/sshconf /home/hadoop/.ssh/config

# Copy the hadoop dir to all servers
pdcp -r -l root -w^./addresses.txt $HADOOP_HOME/* /home/hadoop/
pdsh -l root -w^./addresses.txt chown -R hadoop:hadoop /home/hadoop

# Create the directories mentioned in the hadoop/conf files
pdsh -l root -w^./addresses.txt mkdir -p /mnt/hadoop/tmp
pdsh -l root -w^./addresses.txt mkdir -p /mnt/hadoop/mapred/system
pdsh -l root -w^addresses.txt mkdir -p /mnt/mapredLocal
pdsh -l root -w^addresses.txt mkdir -p /home/hadoop/mapredLocal

#set ownership on these hadoop files 
pdsh -l root -w^./addresses.txt chown hadoop:hadoop -R \
 /home/hadoop /mnt/hadoop /mnt/mapredLocal

# Format hadoop dfs and start hadoop cluster
ssh $QUIET_SSH_OPTIONS -i ./hadoop_dsa \
 hadoop@$(cat master.txt) "bash -i " <<EOF
cd
echo working dir is \$PWD
yes Y | bin/hadoop namenode -format
sleep 5
bin/start-all.sh
EOF

echo 
echo "See job tracker at http://$(cat master.txt):50030 "
