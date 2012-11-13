#!/bin/bash

SPARQLURL=http://bdc34-dev.library.cornell.edu:8890/sparql 

BATCHSIZE=10000

COUNTQUERY='select count(distinct(?uri)) { ?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://marcrdf.library.cornell.edu/canonical/0.1/BibliographicRecord> . }'

#TOTALBIBS=$( curl -g --data-urlencode "query=$COUNTQUERY" $SPARQLURL )
TOTALBIBS=2789030

echo "total number or bib records to load: $TOTALBIBS"
echo $BASH_VERSION
for (( OFFSET=0; OFFSET<$TOTALBIBS ;OFFSET=$OFFSET+$BATCHSIZE ))
do
    BIBURIQUERY="select ?uri { ?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://marcrdf.library.cornell.edu/canonical/0.1/BibliographicRecord> . } LIMIT $BATCHSIZE OFFSET $OFFSET"
  echo $BIBURIQUERY
done
