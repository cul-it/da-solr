#!/usr/bin/python

# Request the startup of euca instances and wait for them to boot.

# usage:
# blocking-start-instances.py "(1,'m1.small'),(4,'m1.large')"

import sys, time
#from subprocess import call,check_output
import subprocess
from types import *

#if you need to change the key change it here. 
#Consider making this configurable 
privateKeyFile = "mykey"

#If you need to change the disk EMI change it here.
#Consider making this configurable 
diskImageEMI = "emi-E1200B2A"

def main():
  if( len(sys.argv) == 1):
    printHelpAndExit()    
  else:
    blockinigStartOfVMs( sys.argv[1] )
    
def blockinigStartOfVMs( cmdStr ):    
    vmsToStart = eval( cmdStr )
    assert isinstance( vmsToStart, tuple), " vmsToStart is not a tuple: %r" % vmsToStart
    
    # if the first thing is an int, then we have one tuple
    if isinstance(vmsToStart[0] , int)  :        
        numberOfVmsToStart = vmsToStart[0]
        size = vmsToStart[1]
        startVMs( numberOfVmsToStart, size)
    else:
        for vmsToStart in vmsToStart:
            numberOfVmsToStart = vmsToStart[0]
            size = vmsToStart[1]
            startVMs( numberOfVmsToStart, size )
                        
    waitForStartup()
    sys.exit(0)
    
def startVMs( number, size ):    
    assert type(number) is IntType , "number of VMs to start should be an int: %s" % number
    assert type( size) is StringType, "size should be a string such as m1.small: %s" % size
    cmd = ["euca-run-instances", "-n", str(number), "-k", privateKeyFile, "-t", size, diskImageEMI]
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
            return        
        time.sleep( 60 )#sec         
    
def printHelpAndExit():    
    print("")
    print(" blocking start up of euca VMs")
    print("   blocking-start-instances.py command ")
    print("   command should be something like: \"(1,'m1.small'),(4,'m1.large')\" ")
    print("   to start one small VM and 4 large VMs. ")
    print("   or something like: \"(4,'m1.large')\" ")
    print("   to start 4 large VMs. ")
    print("")
    print(" The eucalyptus env config script eucarc must already have been sourced.")
    print("")
    print(" Notice that this blocks until there are no pending VMs.") 
    print("This may cause problems if there are other request concurrently made to euca.")    
    print("")
    sys.exit(1)
    
if __name__ == "__main__":
    main( )
