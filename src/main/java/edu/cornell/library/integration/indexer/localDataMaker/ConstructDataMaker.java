package edu.cornell.library.integration.indexer.localDataMaker;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import edu.cornell.library.integration.utilities.IndexingUtilities;
import edu.cornell.mannlib.vitro.webapp.rdfservice.ChangeSet;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService.ModelSerializationFormat;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceException;

public class ConstructDataMaker implements LocalDataMaker{
	
	List<String> SPARQLConstructs = new ArrayList<>();
	
	public LocalDataMaker addSPARQLConstruct(String construct){
		this.SPARQLConstructs.add(construct);
		return this;
	}
	
	@Override
	public void gather(String recordURI, RDFService mainStore, RDFService localStore) 
			throws RDFServiceException, IOException {
				
		ModelSerializationFormat format = RDFService.ModelSerializationFormat.N3;
		
		for( String construct : SPARQLConstructs ){			
			ChangeSet changes = localStore.manufactureChangeSet();
			
			String query = IndexingUtilities.substituteInRecordURI(recordURI, construct);
			try ( InputStream is = mainStore.sparqlConstructQuery(query, format) ) {
			
				changes.addAddition(is, format, (String)null);
			}
			localStore.changeSetUpdate(changes);
		}
							
	}

}
