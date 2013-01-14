#!/bin/bash
CLASS=edu.cornell.library.integration.ConvertBibDailyToXml
CLASSES=./build/classes
LIB=./build/lib
OPTS=-Xmx1536m
CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES
# need to specifiy src and destination Dir for bib data
# java -classpath $CLASSPATH $CLASS $@
SRCDIR=http://culdata.library.cornell.edu/data/voyager/bib/bib.mrc.daily
DESTDIR=http://culdata.library.cornell.edu/data/voyager/bib/bib.xml.daily
java $OPTS -classpath $CLASSPATH $CLASS $SRCDIR $DESTDIR
