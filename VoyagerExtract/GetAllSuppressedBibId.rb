#!/usr/bin/env jruby
#
# GetAllSuppressedBibId
#
require "java"
$CLASSPATH << 'build/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetAllSuppressedBibId'
print "GetAllSuppressedBibId...\n" 
destdir = "http://culdata.library.cornell.edu/data/voyager/bib/suppressed" 
args = [destdir].to_java(:string)
GetAllSuppressedBibId.main(args)
