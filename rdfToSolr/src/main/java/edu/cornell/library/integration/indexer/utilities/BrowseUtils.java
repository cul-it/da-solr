package edu.cornell.library.integration.indexer.utilities;


public class BrowseUtils {
	
	public static enum HeadTypeDesc {
		PERSNAME("Personal Name"),
		CORPNAME("Corporate Name"),
		EVENT("Event"),
		GENHEAD("General Heading"),
		TOPIC("Topical Term"),
		GEONAME("Geographic Name"),
		CHRONTERM("Chronological Term"),
		GENRE("Genre/Form Term"),
		MEDIUM("Medium of Performance");
		
		private String string;
		
		private HeadTypeDesc(String name) {
			string = name;
		}

		public String toString() { return string; }
	}
	
	public static class BlacklightField {
		private RecordSet _rs;
		private HeadType _ht;
		private HeadTypeDesc _htd;
		private String _field;
		private String _facet;
		
		
		public BlacklightField(RecordSet rs, HeadType ht, HeadTypeDesc htd,String field, String facetField) {
			_ht = ht;
			_htd = htd;
			_field = field;
			_facet = facetField;
			_rs = rs;
		}
		public RecordSet recordSet() { return _rs; }
		public HeadType headingType() { return _ht; }
		public HeadTypeDesc headingTypeDesc() { return _htd; }
		public String fieldName() { return _field; }
		public String facetField() { return _facet; }
		
	}

	
	public static enum HeadType {
		AUTHOR("author","works_by"),
		SUBJECT("subject","works_about"),
		AUTHORTITLE("authortitle","works");
		
		private String string;
		private String field;
		
		private HeadType(String name, String dbField) {
			string = name;
			field = dbField;
		}

		public String toString() { return string; }
		public String dbField() { return field; }
	}	

	public static enum RecordSet {
		NAME("name"),
		SUBJECT("subject"),
		SERIES("series"), /*Not currently implementing series header browse*/
		NAMETITLE("nametitle");
		private String string;
		
		private RecordSet(String name) {
			string = name;
		}

		public String toString() { return string; }
	}

	public static enum ReferenceType {
		TO4XX("alternateForm"),
		FROM4XX("preferedForm"),
		TO5XX("seeAlso"),
		FROM5XX("seeAlso");
		
		private String string;
		
		private ReferenceType(String name) {
			string = name;
		}

		public String toString() { return string; }
		
	}

}
