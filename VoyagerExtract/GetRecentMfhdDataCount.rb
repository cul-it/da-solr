#!/usr/bin/env jruby
#
# GetRecentMfhdDataCount
#
require "java"
$CLASSPATH << 'build/classes/'
$CLASSPATH << 'resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetRecentMfhdDataCount'
print "GetRecentMfhdDataCount...\n" 
 
GetRecentMfhdDataCount.main()
