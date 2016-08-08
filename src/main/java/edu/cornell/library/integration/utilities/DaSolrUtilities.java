package edu.cornell.library.integration.utilities;

public class DaSolrUtilities {

	public static enum CurrentDBTable {
		BIB_VOY("bibRecsVoyager"),
		MFHD_VOY("mfhdRecsVoyager"),
		ITEM_VOY("itemRecsVoyager"),
		BIB_SOLR("bibRecsSolr"),
		MFHD_SOLR("mfhdRecsSolr"),
		ITEM_SOLR("itemRecsSolr"),
		BIB2WORK("bib2work"),
		QUEUE("indexQueue"),
		BATCHLOCK("batchLock");

		private String string;

		private CurrentDBTable(String name) {
			string = name;
		}
		public String toString() { return string; }
	}

}
