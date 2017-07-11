package edu.cornell.library.integration.indexer.utilities;

import edu.cornell.library.integration.indexer.solrFieldGen.*;

/**
 * An enumeration of classes implementing SolrFieldGenerator to be used by
 * GenerateSolrFields.
 */
public enum Generator {
	AUTHORTITLE( AuthorTitle.class ),
	SUBJECT(     Subject.class ),
	PUBINFO(     PubInfo.class ),
	TOC(         TOC.class),
	MARC(        MARC.class),
	SIMPLEPROC(  SimpleProc.class ),
	CITATIONREF( CitationReferenceNote.class )
	;

	public SolrFieldGenerator getInstance() { return this.generator; }

	private final SolrFieldGenerator generator;
	private Generator( final Class<? extends SolrFieldGenerator> generatorClass ) {
		SolrFieldGenerator tmp = null;
		try { tmp = generatorClass.newInstance(); } 
		catch (InstantiationException | IllegalAccessException e) { e.printStackTrace(); }
		this.generator = tmp;
	}
}
