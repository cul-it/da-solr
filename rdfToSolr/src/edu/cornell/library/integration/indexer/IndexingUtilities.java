package edu.cornell.library.integration.indexer;

public class IndexingUtilities {
	
	public static String substitueInRecordURI(String recordURI, String query) {
		if( query == null )
			return null;			
		return query.replaceAll("\\$recordURI\\$", "<"+recordURI+">");		
	}

}
