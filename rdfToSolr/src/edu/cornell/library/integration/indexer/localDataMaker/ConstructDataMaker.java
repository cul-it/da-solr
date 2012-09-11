package edu.cornell.library.integration.indexer.localDataMaker;

import java.util.ArrayList;
import java.util.List;

import edu.cornell.mannlib.vitro.webapp.rdfservice.ChangeSet;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFQueryService;

public class ConstructDataMaker implements LocalDataMaker{
	
	List<String> SPARQLConstructs = new ArrayList<String>();
	
	public LocalDataMaker addSPARQLConstruct(String construct){
		this.SPARQLConstructs.add(construct);
		return this;
	}
	
	@Override
	public ChangeSet gather(String recordURI, RDFQueryService mainStore) {
		
	}

}
