#!/usr/bin/env jruby
#
# IncrementalUpdate
#
require "java"
$CLASSPATH << 'VoyagerExtract/build/classes/'
$CLASSPATH << 'VoyagerExtract/resources/'
Dir["./lib/\*.jar"].each { |jar| require jar }

# print $CLASSPATH
java_import 'edu.cornell.library.integration.GetCombinedUpdatesMrc'
print "GetCombinedUpdatesMrc...\n"
bibdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.mrc.updates" 
mfhddir = "http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.mrc.updates" 

args = [bibdir,mfhddir].to_java(:string)
GetCombinedUpdatesMrc.main(args)

java_import 'edu.cornell.library.integration.ConvertBibUpdatesToXml'

srcdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.mrc.updates"
destdir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.xml.updates" 
args = [srcdir,destdir].to_java(:string)
ConvertBibUpdatesToXml.main(args)

java_import 'edu.cornell.library.integration.ConvertMfhdUpdatesToXml'

srcdir = "http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.mrc.updates"
destdir = "http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.xml.updates" 
args = [srcdir,destdir].to_java(:string)
ConvertMfhdUpdatesToXml.main(args)
