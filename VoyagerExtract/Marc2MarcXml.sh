#!/bin/bash
#
# Run Marc2MarcXML
#
CLASS=edu.cornell.library.integration.Marc2MarcXML
CLASSES=./build/classes
LIB=./build/lib


CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

# Need to specify src and dest dir on dav
SRCDIR=http://jaf30-dev.library.cornell.edu/data/voyager/bib/bib.mrc.updates
DESTDIR=http://jaf30-dev.library.cornell.edu/data/voyager/bib/bib.xml.updates

java -classpath $CLASSPATH $CLASS $SRCDIR $DESTDIR

# java -classpath $CLASSPATH $CLASS $@
