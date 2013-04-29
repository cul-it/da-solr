#!/usr/bin/env jruby
#
# GetUnSuppressedBibInfo
#
require "java"
$CLASSPATH << 'build/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetUnSuppressedBibInfo'
print "GetUnSuppressedBibInfo...\n" 
destdir = "http://culdata.library.cornell.edu/data/voyager/bib/unsuppressed" 
args = [destdir].to_java(:string)
GetUnSuppressedBibInfo.main(args)