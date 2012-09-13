package edu.cornell.library.integration.indexer;

import static edu.cornell.library.integration.indexer.IndexingUtilities.*;

import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.SolrInputDocument;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import edu.cornell.library.integration.indexer.fieldMaker.FieldMaker;
import edu.cornell.library.integration.indexer.localDataMaker.LocalDataMaker;
import edu.cornell.mannlib.vitro.webapp.rdfservice.ChangeListener;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceException;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceFactory;
import edu.cornell.mannlib.vitro.webapp.rdfservice.impl.jena.model.RDFServiceModel;

/**
 * This class is intended to be configured to convert a record 
 * into a SolrInputDocument by querying RDF sources. 
 */
public abstract class RecordToDocumentBase implements RecordToDocument{
		
	/** list of objects to build the fields of the solr doc. */
	abstract List<? extends FieldMaker> getFieldMakers();	
	
	/** Factory for empty local in-memory RDF stores. */
	RDFServiceFactory getLocalStoreFactory(){		
		return new RDFServiceFactory(){
			@Override
			public RDFService getRDFService() {
				Model model = ModelFactory.createDefaultModel();
				return new RDFServiceModel(model);
			}

			@Override
			public void registerListener(ChangeListener changeListener)
					throws RDFServiceException {
				throw new RDFServiceException("registerListener is not implemented");
			}

			@Override
			public void unregisterListener(ChangeListener changeListener)
					throws RDFServiceException { 
				throw new RDFServiceException("unregisterListener is not implemented");				
			}			
		};		
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
			RDFService mainStorQueryService) throws RDFServiceException {	
						
		//construct local graph
		RDFService localStore = null;
		if( getLocalStoreFactory() != null && getLocalDataMakers() != null ){
			 localStore = getLocalStoreFactory().getRDFService();
			if( localStore != null ){
				for( LocalDataMaker con : getLocalDataMakers()){
					con.gather(recordURI, mainStorQueryService, localStore);								
				}
			}
		}
		
		//get all the fields
		SolrInputDocument doc = new SolrInputDocument();
		for( FieldMaker maker : getFieldMakers()){
			try {
				//this needs to merge the values from fields with the same key
				combineFields( doc, maker.buildFields(recordURI,mainStorQueryService, localStore));				
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
