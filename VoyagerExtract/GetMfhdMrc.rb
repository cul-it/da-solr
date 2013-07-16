#!/usr/bin/env jruby
#
# GetMfhdMrc
#
require "java"
$CLASSPATH << 'build/classes/'
$CLASSPATH << 'resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetMfhdMrc'
print "Getting Mfhd Master Data...\n"
mfhdid = ARGV[0]
destdir = "http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.mrc.updates"
args = [mfhdid, destdir].to_java(:string)
GetMfhdMrc.main(args)
