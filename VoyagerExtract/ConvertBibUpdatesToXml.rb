#!/usr/bin/env jruby
#
# ConvertBibUpdatesToXml
#
require "java"
$CLASSPATH << 'build/classes/'
$CLASSPATH << 'resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.ConvertBibUpdatesToXml'
print "ConvertBibUpdatesToXml...\n" 
srcdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.mrc.updates"
destdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.xml.updates" 
args = [srcdir,destdir].to_java(:string)
ConvertBibUpdatesToXml.main(args)
