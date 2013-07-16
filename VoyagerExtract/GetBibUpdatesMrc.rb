#!/usr/bin/env jruby
#
# GetBibUpdatesMrc
#
require "java"
$CLASSPATH << 'build/classes/'
$CLASSPATH << 'resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetBibUpdatesMrc'
print "GetBibUpdatesMrc...\n"
destdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.mrc.updates" 
args = [destdir].to_java(:string)
GetBibUpdatesMrc.main(args)
