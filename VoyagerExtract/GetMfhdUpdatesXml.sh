#!/bin/bash
CLASS=edu.cornell.library.integration.GetMfhdUpdatesXml
CLASSES=./build/classes
LIB=./build/lib
OPTS=-Xmx1280m

CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

# need to specifiy  destination Dir for mfhd data
# java -classpath $CLASSPATH $CLASS $@
DESTDIR=http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.xml.updates
java $OPTS -classpath $CLASSPATH $CLASS $DESTDIR
