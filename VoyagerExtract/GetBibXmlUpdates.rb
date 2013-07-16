#!/usr/bin/env jruby
#
# GetBibUpdatesXml
#
require "java"
$CLASSPATH << 'build/classes/'
$CLASSPATH << 'resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetBibUpdatesXml'
print "GetBibUpdatesXml...\n"
destdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.xml.updates" 
args = [destdir].to_java(:string)
GetBibUpdatesXml.main(args)
