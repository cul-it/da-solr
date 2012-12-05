package edu.cornell.library.integration.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hp.hpl.jena.rdf.model.Model;

import edu.cornell.library.integration.ILData.ILData;
import edu.cornell.library.integration.ILData.ILDataRemoteImpl;
import edu.cornell.mannlib.vitro.webapp.rdfservice.impl.sparql.RDFServiceSparqlHttp;


/**
 * For a given Bib URI, get the RDF from a remote service.
 * 
 * @author bdc34
 *
 * @param <K>
 */
public class BibURIToRdfLocalMapper  <K> extends Mapper<K, Text, Text, Text>{ 
	
String sparqlUrl = null;
	
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		sparqlUrl = conf.get( MarcToSolrPrototype.REMOTE_SPARQL_ENDPOINT );		
		if( sparqlUrl == null )			
			throw new Error("BibURIToRdfLocalMapper must have a "+ 
					MarcToSolrPrototype.REMOTE_SPARQL_ENDPOINT + " property in the job configuration");
			
	}

	public void map(K key, Text value, Context context) throws IOException, InterruptedException {
				
		String bibURI = value.toString();

		ILData data = new ILDataRemoteImpl( new RDFServiceSparqlHttp(sparqlUrl) );
		Model m = data.getBibData( bibURI );
							
		context.write(new Text( bibURI ), 
				   	  new Text( MarcToSolrUtils.writeModelToNTString(m)));
	}
	
}
