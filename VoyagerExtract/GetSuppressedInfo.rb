#!/usr/bin/env jruby
#
# Get Suppressed and Unsuppressed Ids
#
require "java"
$CLASSPATH << 'build/classes/'
$CLASSPATH << 'resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
print "GetAllSuppressedBibId...\n"
java_import 'edu.cornell.library.integration.GetAllSuppressedBibId'
destdir="http://culdatadev.library.cornell.edu/data/voyager/bib/suppressed"
args = [destdir].to_java(:string)
GetAllSuppressedBibId.main(args)

print "GetAllUnSuppressedBibId...\n"
java_import 'edu.cornell.library.integration.GetAllUnSuppressedBibId'
destdir="http://culdatadev.library.cornell.edu/data/voyager/bib/unsuppressed"
args = [destdir].to_java(:string)
GetAllUnSuppressedBibId.main(args)

print "GetAllSuppressedMfhdId...\n"
java_import 'edu.cornell.library.integration.GetAllSuppressedMfhdId'
destdir="http://culdatadev.library.cornell.edu/data/voyager/mfhd/suppressed"
args = [destdir].to_java(:string)
GetAllSuppressedMfhdId.main(args)

print "GetAllUnSuppressedMfhdId...\n"
java_import 'edu.cornell.library.integration.GetAllUnSuppressedMfhdId'
destdir="http://culdatadev.library.cornell.edu/data/voyager/mfhd/unsuppressed"
args = [destdir].to_java(:string)
GetAllUnSuppressedMfhdId.main(args)

