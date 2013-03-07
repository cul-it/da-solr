#!/usr/bin/python

# Makes files with addresses from euca-describe-instances.
# It make the following files:
#   ./addresses.txt
#   ./hosts
#   ./masters.txt
#   ./slaves.txt
#   
# if the directory ./hadoop exits it will make
# ./hadoop/conf/masters
# ./hadoop/conf/slaves
#
# The master node will be the smallest VM.  If there is no single smallest VM, the an error be thrown.

import sys, os, subprocess
from types import *
from optparse import OptionParser

sizes = ['m1.small', 'c1.medium', 'm1.large', 'm1.xlarge', 'c1.xlarge' ]

def main():
    parser = OptionParser()
    parser.add_option("-t", "--testFile", dest="testfile", action="store", type="string",
                      help="use content of test file instead of call euca-describe-instances ")

    (options,args) = parser.parse_args()
        
    vms = getVMInfo( options.testfile )        
    checkForMaster( vms )
    makeAddressesTxt( vms )    
    makeHosts( vms )
    makeHostnameScript( vms )
    makeHadoopConfs( vms )      
  
def getVMInfo( testfile=None ) :
    if testfile == None:
        desc = runEucaDesc()
    else:
        with open(testfile,'r') as f:
            desc = f.read()
              
    vmValues = map( str.split , desc.splitlines() )
    
    #keep only instances
    vmValues = filter ( lambda x: len(x) > 0 and x[0].find("INSTANCE") > -1 , vmValues )
    
    #make a list, one item per vm, where the item is a map of vm values
    keys = ['type','id','emi','ip','ip2nd','state','key','unknown','size','date','zone','kernel','ramdisk']
    return map( lambda values: dict(zip(keys,values)) , vmValues)  

def runEucaDesc():
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

    	return output

def checkForMaster(vms):
    """check for single smallest VM, exit if there isn't one"""
    indexOfSmallest = getSmallestVM( vms )
    
    if indexOfSmallest < 0 :
        print(" Could not find smallest VM to use as master" )
        sys.exit(1)            
    
def getSmallestVM( vms):
    """ return the index of the smallest VM or -1 if there isn't one or -2 if there are multiple """
    sizeOfSmallest = getSizeOfSmallestVM(vms)
    if sizeOfSmallest < 0 :
        return sizeOfSmallest
    else:
        for i in range(0,len(vms)):
            if vms[i]['size'] == sizeOfSmallest :
                return i            
    #not sure how we could get here            
    return -1 
    
def getSizeOfSmallestVM( vms ):
    """ returns the size of the smallest vm, or -1 if there  isn't one or -2 if there are multiple """
    
    # make a dictory of vmsize -> 0    
    sizeCounts = dict( zip( sizes, [0] * len(sizes) ))
    
    # count up the vms of the different sizes
    for vm in vms : 
        sizeCounts[vm['size']]= 1 + sizeCounts[vm['size']]

    #Find the lowest single size and return that size name
    for size in sizes:
        vmsOfThisSize = sizeCounts[size]
        if vmsOfThisSize == 0 :
            pass        
        elif vmsOfThisSize == 1 :
            return size
        else : # vmsOfThisSize > 1
            return -2
    
    #none found
    return -1
             
    
def ipToHostname( ip ):
    return "node" + ip[ip.rfind( '.' )+1:]
                 
def makeAddressesTxt( vms ):            
    with open("addresses.txt",'w') as f:
        for vm in vms :
            f.write(  vm['ip'] + '\n' )
                
    master = getSmallestVM( vms )
    if master >= 0 :
        #write master.txt file
        with open("master.txt",'w') as f:
            f.write( vms[master]['ip'] + '\n')
    
        #write slaves.txt file
        with open("slaves.txt",'w') as f:
            [f.write(vms[i]['ip'] + '\n') 
             for i in range(0,len(vms)) 
             if i != master ]                
                
             
def makeHadoopConfs(vms):
    if os.path.isdir( os.path.join('hadoop','conf') ) :
        dir = os.path.join('hadoop','conf')
    else:
        dir = ''

    master = getSmallestVM( vms )
            
    with open(os.path.join(dir,"slaves"),'w') as f:            
        [f.write(ipToHostname(vms[i]['ip']) + '\n') 
         for i in range(0,len(vms)) 
         if i != master ]
    
    with open(os.path.join(dir,"masters"),'w') as f:
        f.write(ipToHostname(vms[master]['ip']) + '\n')
                
additional_hosts = """127.0.0.1        localhost.localdomain localhost
::1        localhost6.localdomain6 localhost6"""

def makeHostnameScript(vms):
    sshopts = " -i $EUCA_KEY_FILE -o StrictHostKeychecking=no -o UserKnownHostsFile=/dev/null "
    with open("hostnames.sh",'w') as f:
        for vm in vms:
            ip = vm['ip']
            f.write( "ssh %s root@%s hostname %s \n " % (sshopts, ip, ipToHostname(ip)) )

def makeHosts(vms):       
    master = getSmallestVM( vms )

    with open("hosts",'w') as f:           
        f.write( additional_hosts + '\n')
        for i in range (0,len(vms)):
            ip = vms[i]['ip']
            if i != master :
                f.write( "%s %s\n" %( ip ,ipToHostname( ip )) )
            else:
                f.write( "%s %s master \n" %( ip ,ipToHostname( ip )) )

if __name__ == "__main__":
    main( )
