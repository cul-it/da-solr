package edu.cornell.library.integration.utilities;

import edu.cornell.library.integration.metadata.support.HeadingCategory;
import edu.cornell.library.integration.metadata.support.HeadingType;

public enum BlacklightHeadingField {

	AUTHOR_PERSON    (HeadingCategory.AUTHOR,      HeadingType.PERSNAME),
	AUTHOR_CORPORATE (HeadingCategory.AUTHOR,      HeadingType.CORPNAME),
	AUTHOR_EVENT     (HeadingCategory.AUTHOR,      HeadingType.EVENT),
	SUBJECT_PERSON   (HeadingCategory.SUBJECT,     HeadingType.PERSNAME),
	SUBJECT_CORPORATE(HeadingCategory.SUBJECT,     HeadingType.CORPNAME),
	SUBJECT_EVENT    (HeadingCategory.SUBJECT,     HeadingType.EVENT),
	AUTHORTITLE_WORK (HeadingCategory.AUTHORTITLE, HeadingType.WORK),
	SUBJECT_WORK     (HeadingCategory.SUBJECT,     HeadingType.WORK),
	SUBJECT_TOPIC    (HeadingCategory.SUBJECT,     HeadingType.TOPIC),
	SUBJECT_PLACE    (HeadingCategory.SUBJECT,     HeadingType.GEONAME),
	SUBJECT_CHRON    (HeadingCategory.SUBJECT,     HeadingType.CHRONTERM),
	SUBJECT_GENRE    (HeadingCategory.SUBJECT,     HeadingType.GENRE);

	private final HeadingCategory hc;
	private final HeadingType ht;

	BlacklightHeadingField(final HeadingCategory hc, final HeadingType ht) {
		this.hc = hc;
		this.ht = ht;
	}

	public HeadingCategory headingCategory() { return hc; }
	public HeadingType headingTypeDesc() { return ht; }
	public String browseCtsName() {
		final StringBuilder sb = new StringBuilder();
		sb.append(hc.toString());
		if ( ! hc.equals(HeadingCategory.AUTHORTITLE) )
			sb.append('_').append(ht.abbrev());
		sb.append("_browse");
		return sb.toString();
	}
	public String fieldName() {
		final StringBuilder sb = new StringBuilder();
		sb.append(hc.toString());
		if ( ! hc.equals(HeadingCategory.AUTHORTITLE) )
			sb.append('_').append(ht.abbrev());
		sb.append("_filing");
		return sb.toString();
	}
	public String facetField() {
		final StringBuilder sb = new StringBuilder();
		sb.append(hc.toString());
		if ( hc.equals(HeadingCategory.SUBJECT) )
			sb.append('_').append(ht.abbrev());
		sb.append("_facet");
		return sb.toString();
	}

}
