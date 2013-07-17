#!/usr/bin/env jruby
#
# GetAllSuppressedBibId
#
require "java"
$CLASSPATH << 'build/classes/'
$CLASSPATH << 'resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.FindMissingBibId'
print "FindMissingBibId...\n" 
FindMissingBibId.main()
