#!/usr/bin/env jruby
#
# GetCombinedUpdatesMrc
#
require "java"
$CLASSPATH << 'build/classes/'
$CLASSPATH << 'resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetCombinedUpdatesMrc'
print "GetCombinedUpdatesMrc...\n"
bibdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.mrc.updates" 
mfhddir = "http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.mrc.updates" 
args = [bibdir,mfhddir].to_java(:string)
GetCombinedUpdatesMrc.main(args)