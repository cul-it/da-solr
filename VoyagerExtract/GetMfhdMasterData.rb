#!/usr/bin/env jruby
#
# GetMfhdMasterData
#
require "java"
$CLASSPATH << 'build/classes/'
$CLASSPATH << 'resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetMfhdMasterData'
print "Getting Mfhd Master Data...\n"
mfhdid = ARGV[0]
args = [mfhdid].to_java(:string)
GetMfhdMasterData.main(args)
