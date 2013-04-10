#!/bin/bash
CLASS=edu.cornell.library.integration.ConvertBibUpdatesToXml
CLASSES=./build/classes
OPTS=-Xmx1280m
LIB=./build/lib
CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

# echo $CLASSPATH
# need to specifiy bibid and destination Dir for bib data
# java -classpath $CLASSPATH $CLASS $@
SRCDIR=http://culdata.library.cornell.edu/data/voyager/bib/bib.mrc.updates
DESTDIR=http://culdata.library.cornell.edu/data/voyager/bib/bib.xml.updates
java $OPTS -classpath $CLASSPATH $CLASS $SRCDIR $DESTDIR
