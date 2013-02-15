#!/usr/bin/python

# This is a script that will query the euclyptus tools
# and setup hosts, slave and master files. 

# It once the eucalyptus cluster is running it will
#  1.  make the /etc/hosts file for each node
#  2. make the hadoop/conf/slaves file
#  3. make the hadoop/conf/masters file
#  4. make the addresses.txt file that can be used with pdsh

import os, sys, re
from urlparse import urlparse

ip_pattern = re.compile("[0-9]*.[0-9]*.[0-9]*.([0-9]*)")

def ipToHostname(ip):
    # something like 128.84.8.112 -> node112
    # this has to match what the vm image is setup to do on starutp in /etc/local.d
    m = ip_pattern.match( ip )
    return "node" + m.group(1)
    
    
def doSetupGuess( argv ):
    print("not yet implemented")
    
def doSetupNonGuess( argv ):
    hadoopDir = argv[1]
    masterIp = argv[2]
    slaveIps = argv[2:]    
    doSetup( hadoopDir, masterIp, slaveIps)
    
def makeHadoopConfFile(file,ips):    
    outf = open( file , 'w')
    
    if isinstance(ips, basestring):
        # ips might just be a string
        outf.write( ipToHostname( ips ) + '\n')
    else:
        # ips might just be a list of strings                    
        for ip in ips:
            outf.write( ipToHostname(ip) + '\n' )
            
    outf.close()
    
        
def makeAddressFile( file,  ips ):
    """ addresses.txt has the ip address of all the nodes for use with pdsh 
        such as pdsh -w^./addresses.txt cmd"""
    out = open( file , 'w')
    for ip in ips: out.write( ip + '\n' )
    out.close()

def makeHostsFile( file,  ips):
    """ hosts has the hostname to ip address for each node.
        for use in /etc/hosts"""
    out = open( file , 'w')

    out.write('127.0.0.1        localhost.localdomain localhost\n')
    out.write('::1        localhost6.localdomain6 localhost6\n')
    for ip in ips: 
        out.write( ip + " " + ipToHostname(ip) + '\n');
        
    out.close()
            
def doSetup( hadoopDir, masterIp, slaveIps):
    if not os.path.exists(os.path.join(hadoopDir ,"conf")) : 
        os.mkdir(os.path.join(hadoopDir ,"conf"))   
    makeHadoopConfFile(os.path.join(hadoopDir , "conf/slaves") , slaveIps)
    makeHadoopConfFile(os.path.join(hadoopDir , "conf/masters"), masterIp)    
    makeAddressFile( os.path.join(hadoopDir , "addresses.txt"), [ masterIp ] + slaveIps )
    makeHostsFile( os.path.join(hadoopDir , "hosts") , [ masterIp ] + slaveIps )        
    
def getConnection():
    """Get a connection to the euca system"""

    # boto can be installed with the command
    # $ pip install boto    
    import boto
    
    # get the keys from the env variables
    acc_key = os.getenv("EC2_ACCESS_KEY")
    secret_key = os.getenv("EC2_SECRET_KEY")
    ec2_url_str = os.getenv("EC2_URL")

    if( acc_key == None or secret_key == None or ec2_url == None) :
        print("Need to have eucalyptus environment setup. \
        Source the eucarc file in your shell before running setupHostFiles.py. ")
        sys.exit(1)    
    
    ec2_url = urlparse(ec2_url_str)    
    region = RegionInfo(name="eucalyptus", endpoint= ec2_url.hostname )
    return boto.connect_ec2( aws_access_key_id= acc_key,
                               aws_secret_access_key= secret_key,
                               is_secure=False,
                               region=region,
                               port=ec2_url.port,
                               path=ec2_url.path)  
    
def main( argv=None ):
    if argv is None:
        argv = sys.argv
    
    if( len(argv) <= 1):
        print('')
        print(" setupHostFiles.py")
        print("   For a running euca cluster create the")
        print("   /etc/hosts, hadoop/conf/slaves hadoop/conf/masters and addresses.txt ")
        print("   files.")
        print("")
        print("  usage: setupHostFiles.py -g ")
        print("  guess what systems should be slaves and masters")
        print("  or")
        print("  usage: setupHostFiles.py hadoopDir masterIP slaveIP1 slaveIP2 ...")
        sys.exit()
  
    if( argv[1] == '-g'):
        doSetupGuess(argv)        
    else:
        doSetupNonGuess( argv )
        
                
if __name__ == "__main__":
    main( )
