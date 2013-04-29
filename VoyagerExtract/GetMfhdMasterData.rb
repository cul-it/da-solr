#!/usr/bin/env jruby
#
# GetMfhdMasterData
#
require "java"
$CLASSPATH << 'build/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetMfhdMasterData'
print "Getting Mfhd Master Data...\n"
mfhdid = "8318301"
args = [mfhdid].to_java(:string)
GetMfhdMasterData.main(args)