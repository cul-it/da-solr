#!/usr/bin/env jruby
#
# GetSuppressedMfhdInfo
#
require "java"
$CLASSPATH << 'build/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetSuppressedMfhdInfo'
print "GetSuppressedMfhdInfo...\n" 
destdir = "http://culdata.library.cornell.edu/data/voyager/mfhd/suppressed" 
args = [destdir].to_java(:string)
GetSuppressedMfhdInfo.main(args)