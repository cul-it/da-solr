#!/usr/bin/env jruby
#
# ConvertBibDailyToXml
#
require "java"
$CLASSPATH << 'build/classes/'
$CLASSPATH << 'resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.ConvertBibToXml'
print "ConvertBibToXml...\n"
bibid = ARGV[0]
srcdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.mrc.updates"
destdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.xml.updates" 
args = [bibid, srcdir,destdir].to_java(:string)
ConvertBibToXml.main(args)
