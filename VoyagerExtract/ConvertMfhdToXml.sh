#!/bin/bash
CLASS=edu.cornell.library.integration.ConvertMfhdToXml
CLASSES=./build/classes
LIB=./build/lib


CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

# need to specifiy bibid and destination Dir for bib data
# java -classpath $CLASSPATH $CLASS $@
MFHDID=1148503
SRCDIR=http://jaf30-dev.library.cornell.edu/data/voyager/mfhd/mfhd.mrc.updates
DESTDIR=http://jaf30-dev.library.cornell.edu/data/voyager/mfhd/mfhd.xml.updates
java -classpath $CLASSPATH $CLASS $MFHDID $SRCDIR $DESTDIR