#!/usr/bin/env jruby
#
# GetUnSuppressedMfhdInfo
#
require "java"
$CLASSPATH << 'build/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetUnSuppressedMfhdInfo'
print "GetUnSuppressedMfhdInfo...\n" 
destdir = "http://culdata.library.cornell.edu/data/voyager/mfhd/unsuppressed" 
args = [destdir].to_java(:string)
GetUnSuppressedMfhdInfo.main(args)