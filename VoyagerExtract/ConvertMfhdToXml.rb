#!/usr/bin/env jruby
#
# ConvertMfhdDailyToXml
#
require "java"
$CLASSPATH << 'build/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.ConvertMfhdToXml'
print "ConvertMfhdToXml...\n"
mfhdid = "1148503" 
srcdir = "http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.mrc.updates"
destdir = "http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.xml.updates" 
args = [mfhdid, srcdir,destdir].to_java(:string)
ConvertMfhdToXml.main(args)