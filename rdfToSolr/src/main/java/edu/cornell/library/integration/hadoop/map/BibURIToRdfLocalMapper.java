package edu.cornell.library.integration.hadoop.map;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hp.hpl.jena.rdf.model.Model;

import edu.cornell.library.integration.ILData.ILData;
import edu.cornell.library.integration.ILData.ILDataRemoteImpl;
import edu.cornell.library.integration.hadoop.MarcToSolrPrototype;
import edu.cornell.library.integration.hadoop.MarcToSolrUtils;
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
	ILData data;
	
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		sparqlUrl = conf.get( MarcToSolrPrototype.REMOTE_SPARQL_ENDPOINT );		
		if( sparqlUrl == null )			
			throw new Error("BibURIToRdfLocalMapper must have a "+ 
					MarcToSolrPrototype.REMOTE_SPARQL_ENDPOINT + " property in the job configuration");
		
		try {
			data = new ILDataRemoteImpl( new RDFServiceSparqlHttp(sparqlUrl) );
		} catch (IOException e) {
			throw new Error("could not setup remote data source" , e);
		}					
	}

	public void map(K key, Text value, Context context) throws IOException, InterruptedException {			
		String bibURI = value.toString();
		context.write(new Text( bibURI ), 
				   	  new Text( MarcToSolrUtils.writeModelToNTString( data.getBibData( bibURI ) )));
	}
	
}
