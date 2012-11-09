package edu.cornell.library.integration.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Map from a URL of a MarcXML file to a set of Solr documents.
 * @param <K>
 */
public class MarcToSolrDocsMapper <K> extends Mapper<K, Text, Text, Text>{ 
	String holdingsSparqlUrl = null;
	
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		holdingsSparqlUrl = conf.get(MarcToSolrDocs.HOLDINGS_SPARQL_URL);		
	}

	public void map(K key, Text value,
			Context context)
	throws IOException, InterruptedException {
		String marcXMLURL = value.toString();
		//get the file and convert to RDF file
		
		//load the RDF to a triple store
		
		//find all bib-records in triple store
		
		//for each bib-record make a solr document and write that out.		
		//context.write(new Text( bibRecordURI), new Text( doc.toXML() ));		
	}
}
