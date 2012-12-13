#!/bin/bash
#
# Run Marc2MarcXML
#
CLASS=org.marc4j.util.MarcXmlWriter 
CLASSES=./build/classes
LIB=./build/lib


CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

# Need to specify src and dest dir on dav
INPUT=bib.mrc.daily
OUTPUT=bib.xml.daily

java -classpath $CLASSPATH $CLASS $SRCDIR $DESTDIR 
