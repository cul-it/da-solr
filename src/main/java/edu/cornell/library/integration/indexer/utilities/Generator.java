package edu.cornell.library.integration.indexer.utilities;

import edu.cornell.library.integration.indexer.solrFieldGen.*;

/**
 * An enumeration of classes implementing SolrFieldGenerator to be used by
 * GenerateSolrFields.
 */
public enum Generator {
	AUTHORTITLE( AuthorTitle.class ),
	SUBJECT(     Subject.class ),
	SIMPLEPROC(  SimpleProc.class );

	public SolrFieldGenerator getInstance() { return this.generator; }
	public String getDbTable() { return this.dbTable; }

	private final SolrFieldGenerator generator;
	private final String dbTable;
	private Generator( final Class<? extends SolrFieldGenerator> generatorClass ) {
		SolrFieldGenerator tmp = null;
		try { tmp = generatorClass.newInstance(); } 
		catch (InstantiationException | IllegalAccessException e) { e.printStackTrace(); }
		this.generator = tmp;
		dbTable = "solr_seg_"+generatorClass.getSimpleName().toLowerCase();
	}
}
