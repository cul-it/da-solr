#!/usr/bin/env jruby
#
# ConvertMfhdFullToXml
#
require "java"
$CLASSPATH << 'build/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.ConvertMfhdFullToXml'
print "ConvertMfhdFullToXml...\n" 
srcdir = "http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.mrc.full"
destdir = "http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.xml.full" 
args = [srcdir,destdir].to_java(:string)
ConvertMfhdFullToXml.main(args)