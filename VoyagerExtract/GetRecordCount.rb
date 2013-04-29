#!/usr/bin/env jruby
#
# GetRecordCount
#
require "java"
$CLASSPATH << 'build/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetRecordCount'
print "GetRecordCount...\n" 
srcdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.mrc.full.done" 
args = [srcdir].to_java(:string)
GetRecordCount.main(args)