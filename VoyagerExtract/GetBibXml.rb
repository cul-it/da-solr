#!/usr/bin/env jruby
#
# GetBibXml
#
require "java"
$CLASSPATH << 'build/classes/'
$CLASSPATH << 'resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetBibXml'
print "GetBibXml...\n" 
bibid = "5430043"
destdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.xml.updates" 
args = [bibid,destdir].to_java(:string)
GetBibXml.main(args)
