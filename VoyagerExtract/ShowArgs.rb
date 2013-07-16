#!/usr/bin/env jruby
require "java"
srcdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.mrc.daily"
destdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.xml.daily"

$CLASSPATH << 'build/classes/'
$CLASSPATH << 'resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }
print "Begin...\n"
java_import 'edu.cornell.library.integration.support.ShowArgs'

args = [srcdir,destdir].to_java(:string)
ShowArgs.main(args)
