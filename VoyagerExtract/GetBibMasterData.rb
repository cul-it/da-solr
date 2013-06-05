#!/usr/bin/env jruby
#
# GetBibMasterData
#
require "java"
$CLASSPATH << 'build/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetBibMasterData'
print "Getting Bib Master Data...\n"
args = ["6376593"].to_java(:string)
GetBibMasterData.main(args)