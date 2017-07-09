package edu.cornell.library.integration.indexer;

import static edu.cornell.library.integration.utilities.IndexingUtilities.combineFields;

import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.SolrInputDocument;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.documentPostProcess.DocumentPostProcess;
import edu.cornell.library.integration.indexer.fieldMaker.FieldMaker;
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
	
	public RecordToDocument setDebug(boolean d){
		debug = d;
		return this;
	}
	boolean debug = false;
	
	/** Factory for empty local in-memory RDF stores. */
	static RDFServiceFactory getLocalStoreFactory(){		
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

	/** list of object to post process the solr document. Return empty
	 * list if not needed. */
	@SuppressWarnings("static-method")
	List<? extends DocumentPostProcess> getDocumentPostProcess(){	
		return Collections.emptyList(); 
	}	
	
	
	@Override
	public SolrInputDocument buildDoc(String recordURI,
			SolrBuildConfig config) throws Exception {	

		if(debug)
			System.out.println("building document for " + recordURI);

		//get all the fields
		SolrInputDocument doc = new SolrInputDocument();
		for( FieldMaker maker : getFieldMakers()){
			if( debug )
				System.out.println("executing: " + maker.getName());
			
			try {
				//this needs to merge the values from fields with the same key
				combineFields( doc, maker.buildFields(recordURI,config));
				
				//that might throw an error, let the error pass up the stack.
				//but log exceptions.
			} catch (Exception e) {
				log.info("Exception while processing " + recordURI ,e);
			}
		}
		
		//Post process the Document
		List<? extends DocumentPostProcess> dpps = getDocumentPostProcess();
		if( dpps != null ){
			for( DocumentPostProcess dpp : dpps){
				dpp.p(recordURI, config, doc);
			}
		}
		
		return doc;
	}	

	Log log = LogFactory.getLog(RecordToDocumentBase.class);
}
