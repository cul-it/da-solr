package edu.cornell.library.integration.indexer.utilities;

import edu.cornell.library.integration.indexer.solrFieldGen.*;

/**
 * An enumeration of classes implementing SolrFieldGenerator to be used by
 * GenerateSolrFields.
 */
public enum Generator {
	AUTHORTITLE( AuthorTitle.class ),
	TITLE130(    Title130.class ),
	SUBJECT(     Subject.class ),
	PUBINFO(     PubInfo.class ),
	FACTFICTION( FactOrFiction.class ),
	LANGUAGE(    Language.class ),
	ISBN(        ISBN.class ),
	SERIES(      TitleSeries.class ),
	TOC(         TOC.class ),
	INSTRUMENTS( Instrumentation.class ),
	MARC(        MARC.class ),
	SIMPLEPROC(  SimpleProc.class ),
	FINDINGAIDS( FindingAids.class ),
	CITATIONREF( CitationReferenceNote.class ),
	RECORDBOOST( RecordBoost.class );

	public SolrFieldGenerator getInstance() { return this.generator; }

	private final SolrFieldGenerator generator;
	private Generator( final Class<? extends SolrFieldGenerator> generatorClass ) {
		SolrFieldGenerator tmp = null;
		try { tmp = generatorClass.newInstance(); } 
		catch (InstantiationException | IllegalAccessException e) { e.printStackTrace(); }
		this.generator = tmp;
	}
}
