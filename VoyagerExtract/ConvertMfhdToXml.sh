#!/bin/bash
CLASS=edu.cornell.library.integration.ConvertMfhdToXml
CLASSES=./build/classes
LIB=./build/lib
OPTS=-Xmx1280m

CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

# need to specifiy bibid and destination Dir for bib data
# java -classpath $CLASSPATH $CLASS $@
MFHDID=1148503
SRCDIR=http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.mrc.updates
DESTDIR=http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.xml.updates
java $OPTS -classpath $CLASSPATH $CLASS $MFHDID $SRCDIR $DESTDIR
