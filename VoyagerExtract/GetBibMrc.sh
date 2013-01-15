#!/bin/bash
CLASS=edu.cornell.library.integration.GetBibMrc
CLASSES=./build/classes
LIB=./build/lib


CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

# need to specifiy bibid and destination Dir for bib data
# java -classpath $CLASSPATH $CLASS $@
BIBID=5430043 
DESTDIR=http://culdata.library.cornell.edu/data/voyager/bib/bib.mrc.updates
java -classpath $CLASSPATH $CLASS $BIBID $DESTDIR
