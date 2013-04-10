#!/bin/bash
CLASS=edu.cornell.library.integration.ConvertBibFullToXml
CLASSES=./build/classes
OPTS=-Xmx1280m
LIB=./build/lib
CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES
echo $CLASSPATH
# need to specifiy bibid and destination Dir for bib data
# java -classpath $CLASSPATH $CLASS $@
SRCDIR=http://culdata.library.cornell.edu/data/voyager/bib/bib.mrc.full
DESTDIR=http://culdata.library.cornell.edu/data/voyager/bib/bib.xml.full
java $OPTS -classpath $CLASSPATH $CLASS $SRCDIR $DESTDIR
