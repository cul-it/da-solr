#!/usr/bin/env jruby
#
# ConvertMfhdUpdatesToXml
#
require "java"
$CLASSPATH << 'build/classes/'
$CLASSPATH << 'resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.ConvertMfhdUpdatesToXml'
print "ConvertMfhdUpdatesToXml...\n" 
srcdir = "http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.mrc.updates"
destdir = "http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.xml.updates" 
args = [srcdir,destdir].to_java(:string)
ConvertMfhdUpdatesToXml.main(args)
