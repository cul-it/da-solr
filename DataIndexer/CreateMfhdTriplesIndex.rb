#!/usr/bin/env jruby
require "java"
datadir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.nt.full"

$CLASSPATH << 'build/WEB-INF/classes/'
Dir["./lib/\*.jar"].each { |jar| require jar }
print "Begin...\n"
java_import 'edu.cornell.library.integration.app.CreateBibTriplesIndex'

args = [datadir].to_java(:string)
CreateBibTriplesIndex.main(args)
