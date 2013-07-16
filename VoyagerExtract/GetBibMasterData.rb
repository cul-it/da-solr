#!/usr/bin/env jruby
#
# GetBibMasterData
#
require "java"
$CLASSPATH << 'build/classes/'
$CLASSPATH << 'resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetBibMasterData'
print "Getting Bib Master Data...\n"
bibid = ARGV[0]
args = [bibid].to_java(:string)
GetBibMasterData.main(args)
