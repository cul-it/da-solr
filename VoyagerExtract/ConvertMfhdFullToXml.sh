#!/bin/bash
CLASS=edu.cornell.library.integration.ConvertMfhdFullToXml
CLASSES=./build/classes
LIB=./build/lib
OPTS=-Xmx1280m

CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

# need to specifiy src and destination Dir for mfhd data
# java -classpath $CLASSPATH $CLASS $@
SRCDIR=http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.mrc.full
DESTDIR=http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.xml.full
java $OPTS -classpath $CLASSPATH $CLASS $SRCDIR $DESTDIR
