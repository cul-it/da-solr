#!/bin/bash
CLASS=edu.cornell.library.integration.GetBibUpdatesMrc
CLASSES=./build/classes
LIB=./build/lib


CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

# need to specifiy bibid and destination Dir for bib data
# java -classpath $CLASSPATH $CLASS $@
DESTDIR=http://jaf30-dev.library.cornell.edu/data/voyager/bib/bib.mrc.updates
java -classpath $CLASSPATH $CLASS $DESTDIR