package edu.cornell.library.integration.ILData;

import com.hp.hpl.jena.rdf.model.Model;

/**
 * Interface to get data about different bib and holding records 
 * from the Integration Layer.
 * 
 * @author bdc34 
 */
public interface ILData {
	
	/** Returns RDF data for a bibliographic record id. 
	 * May return null in case of error. */ 
	Model getBibData( String bibURI);
	
	/** Returns RDF for a holdings record id. 
	 * May return null in case of error. */
	Model getHoldingsData( String holdingsURI );
	
	/** Returns holdings record RDF for a bibliographic record id. 
	 * This may return more than one holding record in the RDF. 
	 * May return null in case of error. */
	Model getHoldingsDataForBib( String bibURI );
	
}
