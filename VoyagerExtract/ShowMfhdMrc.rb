#!/usr/bin/env jruby
#
# ShowMfhdMrc
#
require "java"
$CLASSPATH << 'build/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.support.ShowMfhdMrc'
print "ShowMfhdMrc...\n" 
mfhdid = ARGV[0]
args = [mfhdid].to_java(:string)
ShowBibMrc.main(args)
