#!/usr/bin/python

# Request the startup of euca instances and wait for them to boot.

# usage:
# blocking-start-instances.py "(1,'m1.small'),(4,'m1.large')"

import sys, time
#from subprocess import call,check_output
import subprocess
from types import *

def main():
  if( len(sys.argv) != 5):
    printHelpAndExit()    
  else:
    blockinigStartOfVMs( sys.argv[1] , sys.argv[2], sys.argv[3], sys.argv[4] )
    
def blockinigStartOfVMs( cmdStr, emi, privateKeyName, securityGroup ):
    vmsToStart = eval( cmdStr )
    assert isinstance( vmsToStart, tuple), " vmsToStart is not a tuple: %r" % vmsToStart
    
    # if the first thing is an int, then we have one tuple
    if isinstance(vmsToStart[0] , int)  :        
        numberOfVmsToStart = vmsToStart[0]
        size = vmsToStart[1]
        startVMs( numberOfVmsToStart, size, emi, privateKeyName, securityGroup)
    else:
        for vmsToStart in vmsToStart:
            numberOfVmsToStart = vmsToStart[0]
            size = vmsToStart[1]
            startVMs( numberOfVmsToStart, size, emi, privateKeyName, securityGroup )
                        
    waitForStartup()
    sys.exit(0)
    
def startVMs( number, size, emi , privateKeyName, securityGroup):
    assert type(number) is IntType , "number of VMs to start should be an int: %s" % number
    assert type( size) is StringType, "size should be a string such as m1.small: %s" % size
    cmd = ["euca-run-instances", 
           "-n", str(number), 
           "-k", privateKeyName, 
           "-t", size, 
           "-g", securityGroup, 
           emi]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    process.wait()
    (stdout,stderr) = process.communicate()
    if process.returncode > 0 :
       print("could not run euca-run-instances")
       print("stdout: " + stdout)
       print("stderr: " + stderr)
    
def waitForStartup():
    # run euca-describe-instances and check for PENDING, return when there are no pending VMs
    done = False
    while( not done ):
	# run euca-describe-instances and get output
	process = subprocess.Popen(["euca-describe-instances"],
		stdout=subprocess.PIPE,stderr=subprocess.PIPE)
	process.wait()
	(output,stderr) = process.communicate()
	
	if process.returncode > 0 :
	   print("could not run euca-describe-instances")
	   print( "stdout: " + output )
	   print( "stderr: " + stderr)
	   sys.exit(1)

        if len( output ) == 0 :
            print(" output of euca-describe-instances was too short, not waiting." )
            sys.exit(1)

        done = -1 == output.find("pending")
        if done : 
            print('The VMs have started.')
            return        
        print('.')
        time.sleep( 30 )#sec
    
def printHelpAndExit():    
    print("")
    print(" blocking start up of euca VMs")
    print("   blocking-start-instances.py command emi privateKeyName securityGroup")
    print("   command should be something like: \"(1,'m1.small'),(4,'m1.large')\" ")
    print("   to start one small VM and 4 large VMs. ")
    print("   or something like: \"(4,'m1.large')\" ")
    print("   to start 4 large VMs. ")
    print("")
    print("   emi is the disk image emi to use on the VMs ")
    print("")
    print("   privateKeyName is the name of the private key to set for root ssh access on the VMs ")
    print("   The eucalyptus env config script eucarc must already have been sourced.")
    print("")
    print("   securityGroup is the eucalyptus security group to start the VMs in")
    print("")
    print(" Notice that this blocks until there are no pending VMs.") 
    print("This may cause problems if there are other request concurrently made to euca.")    
    print("")
    sys.exit(1)
    
if __name__ == "__main__":
    main( )
