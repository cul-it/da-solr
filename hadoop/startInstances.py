# Script to start n RedCloud instances and wait for them to boot
# This script returns the IP addresses of the VMs one per a line.
# If there expected numbe of instances already exists then 
# this will just return the IP addresses.

# uses python 2.7 

import sys, os

#This is the image to use for instances
image="i-4BE808A2"


tabSplit = lambda x : x.split('\t')

def outputAddresses(){
    f = os.popen("euca-describe-instances") 
    runningInst = [ line[4] for line 
                    in map(tabSplit, f.readlines())  
                    if runningInstLine(line) ]
    
}

def runningInstLine(line){
    return line[0] == "INSTANCE" and line[1] == image and line[5] == "running"
}

if len(sys.argv) != 2 :
    print "usage: startInstances.py [numOfInstancesToStart]"
    sys.exit(2)

n = sys.argv[1]

# get the number of instances currently running
f = os.popen("euca-describe-instances") 
runningInst = [line for line in map(tabSplit, f.readlines())  if runningInstLine(line) ]

if len( runningInst ) != n :
  #need to start some more instancse
  startInstances( n - len( runningInst ) )

outputAddresses()
