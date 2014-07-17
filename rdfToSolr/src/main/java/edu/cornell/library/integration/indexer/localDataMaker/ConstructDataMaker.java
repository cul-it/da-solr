package edu.cornell.library.integration.indexer.localDataMaker;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import edu.cornell.library.integration.indexer.utilities.IndexingUtilities;
import edu.cornell.mannlib.vitro.webapp.rdfservice.ChangeSet;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService.ModelSerializationFormat;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceException;

public class ConstructDataMaker implements LocalDataMaker{
	
	List<String> SPARQLConstructs = new ArrayList<String>();
	
	public LocalDataMaker addSPARQLConstruct(String construct){
		this.SPARQLConstructs.add(construct);
		return this;
	}
	
	@Override
	public void gather(String recordURI, RDFService mainStore, RDFService localStore) 
			throws RDFServiceException {
				
		ModelSerializationFormat format = RDFService.ModelSerializationFormat.N3;
		
		for( String construct : SPARQLConstructs ){			
			ChangeSet changes = localStore.manufactureChangeSet();
			
			String query = IndexingUtilities.substitueInRecordURI(recordURI, construct);
			InputStream is = mainStore.sparqlConstructQuery(query, format);
			
			changes.addAddition(is, format, (String)null);
			localStore.changeSetUpdate(changes);
		}
							
	}

}
