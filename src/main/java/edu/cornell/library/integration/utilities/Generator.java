package edu.cornell.library.integration.utilities;

import edu.cornell.library.integration.metadata.generator.*;

/**
 * An enumeration of classes implementing SolrFieldGenerator to be used by
 * GenerateSolrFields. Enumeration keys should be at most 24 characters.
 */
public enum Generator { // NO_UCD
	AUTHORTITLE( AuthorTitle.class ),
	TITLE130(    Title130.class ),
	SUBJECT(     Subject.class ),
	PUBINFO(     PubInfo.class ),
	FORMAT(      Format.class ),
	FACTFICTION( FactOrFiction.class ),
	LANGUAGE(    Language.class ),
	ISBN(        ISBN.class ),
	SERIES(      TitleSeries.class ),
	TITLECHANGE( TitleChange.class ),
	TOC(         TOC.class ),
	INSTRUMENTS( Instrumentation.class ),
	MARC(        MARC.class ),
	SIMPLEPROC(  SimpleProc.class ),
	FINDINGAIDS( FindingAids.class ),
	CITATIONREF( CitationReferenceNote.class ),
	URL(         URL.class ),
	HATHILINKS(  HathiLinks.class ),
	NEWBOOKS(    NewBooks.class ),
	RECORDTYPE(  RecordType.class ),
	RECORDBOOST( RecordBoost.class ),
	OTHERIDS(    OtherIDs.class ),
	CALLNUMBER(  CallNumber.class);

	public SolrFieldGenerator getInstance() { return this.generator; }

	private final SolrFieldGenerator generator;
	private Generator( final Class<? extends SolrFieldGenerator> generatorClass ) {
		SolrFieldGenerator tmp = null;
		try { tmp = generatorClass.newInstance(); } 
		catch (InstantiationException | IllegalAccessException e) { e.printStackTrace(); }
		this.generator = tmp;
	}
}
