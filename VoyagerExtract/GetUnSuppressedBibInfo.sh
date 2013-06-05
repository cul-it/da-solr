#!/bin/bash

CLASSES=./build/classes
LIB=./build/lib
OPTS=-Xmx1280m

CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

# need to specifiy bibid and destination Dir for bib data
# java -classpath $CLASSPATH $CLASS $@

CLASS=edu.cornell.library.integration.GetAllUnSuppressedBibId
SRCDIR=http://culdata.library.cornell.edu/data/voyager/bib/suppressed
java $OPTS -classpath $CLASSPATH $CLASS $MFHDID $SRCDIR
