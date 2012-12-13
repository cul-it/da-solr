#!/bin/bash
#
# Run Marc2MarcXML
#
CLASS=edu.cornell.library.integration.Marc2MarcXml
CLASSES=./build/classes
LIB=./build/lib


CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

# Need to specify src and dest dir on dav
SRCDIR=http://culdatadev.library.cornell.edu/data/voyager/bib/bib.mrc.daily
DESTDIR=http://culdatadev.library.cornell.edu/data/voyager/bib/bib.xml.daily

java -classpath $CLASSPATH $CLASS $SRCDIR $DESTDIR 