#!/usr/bin/env jruby
#
# ConvertBibFullToXml
#
require "java"
$CLASSPATH << 'build/classes/'
$CLASSPATH << 'resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.ConvertBibFullToXml'
print "ConvertBibFullToXml...\n" 
srcdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.mrc.full"
destdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.xml.full" 
args = [srcdir,destdir].to_java(:string)
ConvertBibFullToXml.main(args)
