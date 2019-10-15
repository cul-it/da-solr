package edu.cornell.library.integration.indexer.utilities;


public class BrowseUtils {

	public static enum HeadTypeDesc {
		PERSNAME("Personal Name","pers"),
		CORPNAME("Corporate Name","corp"),
		EVENT("Event","event"),
		GENHEAD("General Heading","gen"),
		TOPIC("Topical Term","topic"),
		GEONAME("Geographic Name","geo"),
		CHRONTERM("Chronological Term","era"),
		GENRE("Genre/Form Term","genr"),
		MEDIUM("Medium of Performance","med"),
		WORK("Work","work");

		private final String string;
		private final String abbrev;

		private HeadTypeDesc(final String name,final String abbrev) {
			this.string = name;
			this.abbrev = abbrev;
		}

		@Override
		public String toString() { return this.string; }
		public String abbrev() { return this.abbrev; }
	}

	public static class BlacklightField {
		private final HeadType _ht;
		private final HeadTypeDesc _htd;
		//		private String _field;
		//		private String _facet;


		public BlacklightField(final HeadType ht, final HeadTypeDesc htd) {
			this._ht = ht;
			this._htd = htd;
		}
		public HeadType headingType() { return this._ht; }
		public HeadTypeDesc headingTypeDesc() { return this._htd; }
		public String browseCtsName() {
			final StringBuilder sb = new StringBuilder();
			sb.append(this._ht.toString());
			if ( ! this._ht.equals(HeadType.AUTHORTITLE) )
				sb.append('_').append(this._htd.abbrev());
			sb.append("_browse");
			return sb.toString();
		}
		public String fieldName() {
			final StringBuilder sb = new StringBuilder();
			sb.append(this._ht.toString());
			if ( ! this._ht.equals(HeadType.AUTHORTITLE) )
				sb.append('_').append(this._htd.abbrev());
			sb.append("_filing");
			return sb.toString();
		}
		public String facetField() {
			final StringBuilder sb = new StringBuilder();
			sb.append(this._ht.toString());
			if ( this._ht.equals(HeadType.SUBJECT) )
				sb.append('_').append(this._htd.abbrev());
			sb.append("_facet");
			return sb.toString();
		}
	}


	public static enum HeadType {
		AUTHOR("author","works_by"),
		SUBJECT("subject","works_about"),
		AUTHORTITLE("authortitle","works"),
		TITLE("title","works");

		private final String string;
		private final String field;

		private HeadType(final String name, final String dbField) {
			this.string = name;
			this.field = dbField;
		}

		@Override
		public String toString() { return this.string; }
		public String dbField() { return this.field; }
	}

	public static enum RecordSet {
		NAME("name"),
		SUBJECT("subject"),
		SERIES("series"), /*Not currently implementing series header browse*/
		NAMETITLE("nametitle");
		private final String string;

		private RecordSet(final String name) {
			this.string = name;
		}

		@Override
		public String toString() { return this.string; }
	}

	public static enum ReferenceType {
		TO4XX("alternateForm"),
		FROM4XX("see"),
		TO5XX("seeAlso"),
		FROM5XX("seeAlso");

		private final String string;

		private ReferenceType(final String name) {
			this.string = name;
		}

		@Override
		public String toString() { return this.string; }

	}

}
