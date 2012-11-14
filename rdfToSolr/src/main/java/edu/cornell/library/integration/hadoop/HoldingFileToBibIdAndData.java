package edu.cornell.library.integration.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Takes a n-triples holding file and returns:
 * bibId -> Holdings_RDF_for_bib
 *  
 * @author bdc34
 */
public class HoldingFileToBibIdAndData extends Mapper<Text, Text, Text, Text> {	
	
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();			
	}

	public void map(Text key, Text value,
			Context context)
	throws IOException, InterruptedException {
		String holdingsUrlFile = key.toString();
		
		//get holding file
		
		//load as RDF
		
		//query for all holdingId/BibId pairs.
		
		//for each holdingId/BibId pair do a SPARQL CONSTRUCT
		
		//write constructed RDF to output as n-3 or n-triples
		String bibId = "2343";		
		context.write(new Text( bibId), 
			   	  new Text( "<fakeXml>"+bibId +"</fakeXml>" ));
	}
}
