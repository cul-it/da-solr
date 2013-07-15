#!/usr/bin/env jruby
require "java"
datadir = "http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.nt.full"

$CLASSPATH << 'build/WEB-INF/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }
print "Begin...\n"
java_import 'edu.cornell.library.integration.app.CreateMfhdTriplesIndex'

args = [datadir].to_java(:string)
CreateMfhdTriplesIndex.main(args)
