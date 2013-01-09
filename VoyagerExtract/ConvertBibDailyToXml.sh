#!/bin/bash
CLASS=edu.cornell.library.integration.ConvertBibDailyToXml
CLASSES=./build/classes
LIB=./build/lib


CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

# need to specifiy src and destination Dir for bib data
# java -classpath $CLASSPATH $CLASS $@
SRCDIR=http://jaf30-dev.library.cornell.edu/data/voyager/bib/bib.mrc.daily
DESTDIR=http://jaf30-dev.library.cornell.edu/data/voyager/bib/bib.xml.daily
java -classpath $CLASSPATH $CLASS $SRCDIR $DESTDIR