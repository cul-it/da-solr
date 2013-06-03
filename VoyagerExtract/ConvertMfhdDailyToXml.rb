#!/usr/bin/env jruby
#
# ConvertMfhdDailyToXml
#
require "java"
$CLASSPATH << 'build/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.ConvertMfhdDailyToXml'
print "ConvertMfhdDailyToXml...\n" 
srcdir = "http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.mrc.daily"
destdir = "http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.xml.daily" 
args = [srcdir,destdir].to_java(:string)
ConvertMfhdDailyToXml.main(args)
