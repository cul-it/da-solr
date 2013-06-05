#!/bin/bash
CLASSES=./build/classes
LIB=./build/lib
OPTS=-Xmx1024M
CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES
# need to specifiy bibid and destination Dir for bib data
# java -classpath $CLASSPATH $CLASS $@
CLASS=edu.cornell.library.integration.GetAllSuppressedBibId
SRCDIR=http://culdata.library.cornell.edu/data/voyager/bib/suppressed
java -Xmx1024m -classpath $CLASSPATH $CLASS $MFHDID $SRCDIR
