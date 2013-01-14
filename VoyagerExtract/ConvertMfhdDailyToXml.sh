#!/bin/bash
CLASS=edu.cornell.library.integration.ConvertMfhdDailyToXml
CLASSES=./build/classes
LIB=./build/lib


CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

# need to specifiy src and destination Dir for mfhd data
# java -classpath $CLASSPATH $CLASS $@
SRCDIR=http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.mrc.daily
DESTDIR=http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.xml.daily
java -classpath $CLASSPATH $CLASS $SRCDIR $DESTDIR
