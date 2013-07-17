#!/usr/bin/env jruby
#
# GetAllSuppressedBibId
#
require "java"
$CLASSPATH << 'build/classes/'
$CLASSPATH << 'resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.support.FindMissingBibId'
print "FindMissingBibId...\n" 
args = [""].to_java(:string)
FindMissingBibId.main(args)
