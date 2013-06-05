#!/usr/bin/env jruby
#
# GetMfhdXml
#
require "java"
$CLASSPATH << 'build/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetMfhdXml'
print "GetMfhdXml...\n" 
mfhdid = "371302"
destdir = "http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.xml.updates" 
args = [mfhdid,destdir].to_java(:string)
GetMfhdXml.main(args)