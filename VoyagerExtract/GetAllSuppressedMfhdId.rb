#!/usr/bin/env jruby
#
# GetAllSuppressedMfhdId
#
require "java"
$CLASSPATH << 'build/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetAllSuppressedMfhdId'
print "GetAllSuppressedMfhdId...\n" 
destdir = "http://culdata.library.cornell.edu/data/voyager/mfhd/suppressed" 
args = [destdir].to_java(:string)
GetAllSuppressedMfhdId.main(args)
