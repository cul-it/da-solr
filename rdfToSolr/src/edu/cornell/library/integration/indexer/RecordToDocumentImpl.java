package edu.cornell.library.integration.indexer;

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.SolrInputDocument;

import edu.cornell.mannlib.vitro.webapp.rdfservice.ChangeSet;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFQueryService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceException;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceFactory;

/**
 * This class is intended to be configured to convert a record 
 * into a SolrInputDocument by querying RDF sources. 
 */
public class RecordToDocumentImpl implements RecordToDocument{
	/** Factory for the main RDF triple store. */
	RDFServiceFactory mainStoreFactory;
	
	/** Factory for empty local RDF stores. */
	RDFServiceFactory localStoreFactory;
	
	/** list of objects to construct a local RDF graph */
	List<LocalDataMaker> localDataMakers = new ArrayList<LocalDataMaker>();
	
	/** list of objects to build the fields of the solr doc. */
	List<FieldMaker> fieldMakers = new ArrayList<FieldMaker>();
	
	/** list of object to post process the solr document. */
	List<DocumentPostProcess> documentPostProcess = new ArrayList<DocumentPostProcess>();
	
	
	@Override
	public SolrInputDocument buildDoc(String recordURI,
			RDFQueryService queryService) throws RDFServiceException {
		
		RDFService mainStore = mainStoreFactory.getRDFService();
						
		//construct local graph 
		RDFService localStore = localStoreFactory.getRDFService();
		for( LocalDataMaker con : localDataMakers){
			ChangeSet cs = con.gather(recordURI, mainStore);
			localStore.changeSetUpdate(cs);
		}
		
		//get all the fields
		SolrInputDocument doc = new SolrInputDocument();
		for( FieldMaker builder : fieldMakers){
			doc.putAll( builder.buildFields(recordURI, mainStore, localStore));
		}
		
		//Post process the Document
		for( DocumentPostProcess dpp : documentPostProcess){
			dpp.p(recordURI, mainStore, localStore, doc);
		}
		
		return doc;
	}

}
