#!/usr/bin/env jruby
#
# GetSuppressedBibInfo
#
require "java"
$CLASSPATH << 'build/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetSuppressedBibInfo'
print "GetSuppressedBibInfo...\n" 
destdir = "http://culdata.library.cornell.edu/data/voyager/bib/suppressed" 
args = [destdir].to_java(:string)
GetSuppressedBibInfo.main(args)