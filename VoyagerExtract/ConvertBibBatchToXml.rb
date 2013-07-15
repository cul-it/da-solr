#!/usr/bin/env jruby
#
# ConvertBibBatchDailyToXml
#
require "java"
$CLASSPATH << 'build/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.ConvertBibBatchToXml'
print "ConvertBibBatchToXml...\n"
filename = ARGV[0];
srcdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.mrc.updates"
destdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.xml.updates" 
args = [filename, srcdir,destdir].to_java(:string)
ConvertBibBatchToXml.main(args)
