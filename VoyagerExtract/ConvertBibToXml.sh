#!/bin/bash
CLASS=edu.cornell.library.integration.ConvertBibToXml
CLASSES=./build/classes
LIB=./build/lib
CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES
OPTS=-Xmx1280m
# need to specifiy bibid and destination Dir for bib data
# java -classpath $CLASSPATH $CLASS $@
BIBID=7527693
SRCDIR=http://culdata.library.cornell.edu/data/voyager/bib/bib.mrc.updates
DESTDIR=http://culdata.library.cornell.edu/data/voyager/bib/bib.xml.updates
java $OPTS -classpath $CLASSPATH $CLASS $BIBID $SRCDIR $DESTDIR
