package edu.cornell.library.integration.utilities;

public class DaSolrUtilities {

	public static enum CurrentDBTable {

		RECORD_CHECK("recordCheckDate"),
		BIB_VOY("recordBib"),
		MFHD_VOY("recordMfhd"),
		ITEM_VOY("recordItem"),
		CIRC_VOY("recordCirc"),

		GEN_Q("generationQueue"),

		BIB_SOLR("bibRecsSolr"),
		MFHD_SOLR("mfhdRecsSolr"),
		ITEM_SOLR("itemRecsSolr"),
		BIB2WORK("bib2work"),
		QUEUE("indexQueue");

		private String string;

		private CurrentDBTable(String name) {
			string = name;
		}
		public String toString() { return string; }
	}

}
