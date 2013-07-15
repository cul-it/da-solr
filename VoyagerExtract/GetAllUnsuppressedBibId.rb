#!/usr/bin/env jruby
#
# GetAllUnSuppressedBibId
#
require "java"
$CLASSPATH << 'build/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetAllUnSuppressedBibId'
print "GetAllUnSuppressedBibId...\n" 
destdir = "http://culdata.library.cornell.edu/data/voyager/bib/unsuppressed" 
args = [destdir].to_java(:string)
GetAllUnSuppressedBibId.main(args)
