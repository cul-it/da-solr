#!/bin/bash

CLASSES=./build/classes
LIB=./build/lib
OPTS=-Xmx1280m

CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

# need to specifiy bibid and destination Dir for bib data
# java -classpath $CLASSPATH $CLASS $@

CLASS=edu.cornell.library.integration.GetAllUnSuppressedMfhdId
SRCDIR=http://culdata.library.cornell.edu/data/voyager/mfhd/unsuppressed
java $OPTS -classpath $CLASSPATH $CLASS $MFHDID $SRCDIR
