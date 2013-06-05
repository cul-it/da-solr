#!/usr/bin/env jruby
#
# GetMfhdUpdatesMrc
#
require "java"
$CLASSPATH << 'build/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetMfhdUpdatesMrc'
print "GetMfhdUpdatesMrc...\n"
destdir = "http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.mrc.updates" 
args = [destdir].to_java(:string)
GetMfhdUpdatesMrc.main(args)