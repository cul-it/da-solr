#!/usr/bin/env jruby
#
# ShowBibMrc
#
require "java"
$CLASSPATH << 'build/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.support.ShowBibMrc'
print "ShowBibMrc...\n" 
bibid = ARGV[0]
args = [bibid].to_java(:string)
ShowBibMrc.main(args)
