#!/usr/bin/env jruby
#
# GetLocations
#
require "java"
$CLASSPATH << 'build/classes/'
$CLASSPATH << 'resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetLocations'
print "GetLocations...\n" 
destdir = "http://culdata.library.cornell.edu/data/voyager/locations"
args = [destdir].to_java(:string)
GetLocations.main(args)
