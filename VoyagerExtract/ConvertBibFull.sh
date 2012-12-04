#!/bin/bash
CLASS=edu.cornell.library.integration.ConvertBibFull
CLASSES=./build/classes
LIB=./build/lib


CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

# need to specifiy bibid and destination Dir for bib data
# java -classpath $CLASSPATH $CLASS $@
SRCDIR=http://culdatadev.library.cornell.edu/data/voyager/bib/bib.mrc.full 
DESTDIR=http://culdatadev.library.cornell.edu/data/voyager/bib/bib.xml.full 
java -classpath $CLASSPATH $CLASS $SRCDIR $DESTDIR
