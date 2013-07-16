#!/usr/bin/env jruby
#
# GetBibBatchMrc
#
require "java"
$CLASSPATH << 'build/classes/'
$CLASSPATH << 'resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetBibBatchMrc'
print "GetBibBatchMrc...\n" 
filename = ARGV[0]
destdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.mrc.updates" 
args = [filename,destdir].to_java(:string)
GetBibBatchMrc.main(args)
