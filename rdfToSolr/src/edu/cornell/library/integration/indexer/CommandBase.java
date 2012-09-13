package edu.cornell.library.integration.indexer;

import java.lang.reflect.Constructor;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

public class CommandBase {

	protected static String toString(SolrInputDocument doc) {
		String out ="SolrInputDocument[\n" ;
		for( String name : doc.getFieldNames()){
			SolrInputField f = doc.getField(name);
			out = out + "  " + name +": '" + f.toString() + "'\n";
		}
		return out + "]\n";						
	}

	protected static RecordToDocument getRecordToDocumentImpl(String recToDocImplClassName) {
		try{
			Class recToDocImplClass = Class.forName(recToDocImplClassName);
			Constructor zeroArgCons = recToDocImplClass.getConstructor(null);
			return (RecordToDocument) zeroArgCons.newInstance(null);			
		}catch(Exception ex){
			System.err.println("could not instanciate class " + recToDocImplClassName);
			System.err.print( ex.toString());
			ex.printStackTrace(System.err);
			System.exit(1);
		}	
		return null;
	}

}
