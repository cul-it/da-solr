#!/usr/bin/env jruby
require "java"

$CLASSPATH << 'build/WEB-INF/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }
print "Begin...\n"
java_import 'edu.cornell.library.integration.app.LoadTriplesIndex'

args = [''].to_java(:string)
LoadTriplesIndex.main(args)
