#!/usr/bin/env jruby
#
# GetRecentBibDataCount
#
require "java"
$CLASSPATH << 'build/classes/'
$CLASSPATH << 'resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetRecentBibDataCount'
print "GetRecentBibDataCount...\n" 
 
GetRecentBibDataCount.main()
