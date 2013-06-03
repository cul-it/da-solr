#!/usr/bin/env jruby
#
# ConvertBibDailyToXml
#
require "java"
$CLASSPATH << 'build/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.ConvertBibDailyToXml'
print "ConvertBibDailyToXml...\n" 
srcdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.mrc.daily"
destdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.xml.daily" 
args = [srcdir,destdir].to_java(:string)
ConvertBibDailyToXml.main(args)
