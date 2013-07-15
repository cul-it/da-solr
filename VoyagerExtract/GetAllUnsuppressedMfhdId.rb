#!/usr/bin/env jruby
#
# GetUnSuppressedMfhdId
#
require "java"
$CLASSPATH << 'build/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetAllUnSuppressedMfhdId'
print "GetAllUnSuppressedMfhdId...\n" 
destdir = "http://culdata.library.cornell.edu/data/voyager/mfhd/unsuppressed" 
args = [destdir].to_java(:string)
GetAllUnSuppressedMfhdId.main(args)
