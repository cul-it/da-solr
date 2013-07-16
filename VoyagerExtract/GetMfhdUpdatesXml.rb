#!/usr/bin/env jruby
#
# GetMfhdUpdatesXml
#
require "java"
$CLASSPATH << 'build/classes/'
$CLASSPATH << 'resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetMfhdUpdatesXml'
print "GetMfhdUpdatesXml...\n"
destdir = "http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.xml.updates" 
args = [destdir].to_java(:string)
GetMfhdUpdatesXml.main(args)
