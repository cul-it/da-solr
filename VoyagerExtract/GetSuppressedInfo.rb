#!/usr/bin/env jruby
#
# Get Suppressed and Unsuppressed Info
#
require "java"
$CLASSPATH << 'build/classes/'
$CLASSPATH << 'resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
print "GetAllUnSuppressedBibInfo...\n"
java_import 'edu.cornell.library.integration.GetAllSuppressedBibInfo'
destdir=http://culdatadev.library.cornell.edu/data/voyager/bib/suppressed
args = [destdir].to_java(:string)
GetAllSuppressedBibInfo.main(args)

print "GetAllUnSuppressedBibInfo...\n"
java_import 'edu.cornell.library.integration.GetAllUnSuppressedBibInfo'
destdir=http://culdatadev.library.cornell.edu/data/voyager/bib/unsuppressed
args = [destdir].to_java(:string)
GetAllUnSuppressedBibInfo.main(args)

print "GetAllUnSuppressedMfhdInfo...\n"
java_import 'edu.cornell.library.integration.GetAllSuppressedMfhdInfo'
destdir=http://culdatadev.library.cornell.edu/data/voyager/mfhd/suppressed
args = [destdir].to_java(:string)
GetAllSuppressedMfhdInfo.main(args)

print "GetAllUnSuppressedMfhdInfo...\n"
java_import 'edu.cornell.library.integration.GetAllUnSuppressedMfhdInfo'
destdir=http://culdatadev.library.cornell.edu/data/voyager/mfhd/unsuppressed
args = [destdir].to_java(:string)
GetAllUnSuppressedMfhdInfo.main(args)

