#!/usr/bin/env jruby
#
# GetBibMrc
#
require "java"
$CLASSPATH << 'build/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetBibMrc'
print "GetBibMrc...\n" 
# bibid = "5430043"
bibid = ARGV[0]
destdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.mrc.updates" 
args = [bibid,destdir].to_java(:string)
GetBibMrc.main(args)
