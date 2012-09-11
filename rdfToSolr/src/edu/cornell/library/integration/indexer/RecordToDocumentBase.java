package edu.cornell.library.integration.indexer;

import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.SolrInputDocument;

import edu.cornell.library.integration.indexer.fieldMaker.FieldMaker;
import edu.cornell.library.integration.indexer.localDataMaker.LocalDataMaker;
import edu.cornell.mannlib.vitro.webapp.rdfservice.ChangeSet;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFQueryService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceException;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceFactory;

/**
 * This class is intended to be configured to convert a record 
 * into a SolrInputDocument by querying RDF sources. 
 */
public abstract class RecordToDocumentBase implements RecordToDocument{
		
	/** list of objects to build the fields of the solr doc. */
	abstract List<? extends FieldMaker> getFieldMakers();	
	
	/** Factory for empty local RDF stores. Return null if not needed. */
	RDFServiceFactory getLocalStoreFactory(){
		return null;
	}
	
	/** list of objects to construct a local RDF graph. 
	 * Return empty list if no local data is needed. */
	List<LocalDataMaker> getLocalDataMakers(){ 
		return Collections.emptyList(); 
	}		
	
	/** list of object to post process the solr document. Return empty
	 * list if not needed. */
	List<DocumentPostProcess> getDocumentPostProcess(){	
		return Collections.emptyList(); 
	}	
	
	
	@Override
	public SolrInputDocument buildDoc(String recordURI,
			RDFQueryService mainStorQueryService) throws RDFServiceException {	
						
		//construct local graph
		RDFService localStore = null;
		if( getLocalStoreFactory() != null && getLocalDataMakers() != null ){
			 localStore = getLocalStoreFactory().getRDFService();
			if( localStore != null ){
				for( LocalDataMaker con : getLocalDataMakers()){
					ChangeSet cs = con.gather(recordURI, mainStorQueryService);
					localStore.changeSetUpdate(cs);				
				}
			}
		}
		
		//get all the fields
		SolrInputDocument doc = new SolrInputDocument();
		for( FieldMaker maker : getFieldMakers()){
			try {
				doc.putAll( maker.buildFields(recordURI, mainStorQueryService, localStore));
			} catch (Exception e) {
				log.error(e,e);
			}
		}
		
		//Post process the Document
		List<DocumentPostProcess> dpps = getDocumentPostProcess();
		if( dpps != null ){
			for( DocumentPostProcess dpp : dpps){
				dpp.p(recordURI, mainStorQueryService, localStore, doc);
			}
		}
		
		return doc;
	}

	Log log = LogFactory.getLog(RecordToDocumentBase.class);
}
