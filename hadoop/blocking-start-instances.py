#!/usr/bin/python

# Request the startup of euca instances and wait for them to boot.

# usage:
# blocking-start-instances.py "(1,'m1.small'),(4,'m1.large')"

import sys    
from subprocess import call,check_output

#if you need to change the key change it here. 
#Consider making this configurable 
privateKeyFile = "mykey.private"

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
    
    # if the first thing is an int, then we have one tuple
    if isinstance(vmsToStart[0] , int)  :        
        numberOfVmsToStart = vmCmd[0]
        size = vmsToStart[1]
        startVMs( numberOfVmsToStart, size)
    else:
        for vmCmd in vmsToStart:
            numberOfVmsToStart = vmCmd[0]
            size = vmCmd[1]
            startVMs( numberOfVmsToStart, size )
                        
    waitForStartup()
    sys.exit(0)
    
def startVMs( number, size ):    
    print(["euca-run-instances", "-n", number, "-k", privateKeyFile, "-t", size, diskImageEMI])
    #call([])
    
def waitForStartup():
    # run euca-describe-instances and check for PENDING, return when there are no pending VMs
    done = False
    while( not done ):
        output = check_output(["euca-describe-instances"])
        if len( output ) == 0 :
            print(" output of euca-describe-instances was too short, cannot wait" )
            sys.exit(1)
        done = -1 == output.find("pending")
        if done : 
            return        
        time.sleep( 5 )         
    
def printHelpAndExit():    
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
    sys.exit(1)
    
if __name__ == "__main__":
    main( )