package edu.cornell.library.integration.authority.mads;

import java.util.List;
import java.util.Map;

public final class MadsConstants {
	public static final String SUBDIV_URL = "http://id.loc.gov/authorities/subjects/collection_Subdivisions";
	public static final String UNDIFF_URL = "http://id.loc.gov/authorities/names/collection_NamesUndifferentiated";

	public static final String LCNAF_CONTEXT = "http://id.loc.gov/authorities/names/context.json";
	public static final String LCNAF_PREFIX = "http://id.loc.gov/authorities/names/";
	
	public static final String LCSH_CONTEXT = "http://id.loc.gov/authorities/subjects/context.json";
	public static final String LCSH_PREFIX = "http://id.loc.gov/authorities/subjects/";

	public static final String LCGFT_CONTEXT = "http://id.loc.gov/authorities/genreForms/context.json";
	public static final String LCGFT_PREFIX = "http://id.loc.gov/authorities/genreForms/";

	public static final String LCMPT_CONTEXT = "http://id.loc.gov/authorities/performanceMediums/context.json";
	public static final String LCMPT_PREFIX = "http://id.loc.gov/authorities/performanceMediums/";

	public static final String RBMS_CONTEXT = "http://id.loc.gov/vocabulary/rbmscv/context.json";
	public static final String RBMS_PREFIX = "http://id.loc.gov/vocabulary/rbmscv/";

	public static final List<String> CONTEXT_PREFIXES = List.of(LCNAF_PREFIX, LCSH_PREFIX, LCGFT_PREFIX, LCMPT_PREFIX, RBMS_PREFIX);


	public static final String HAS_BROADER_AUTHORITY = "madsrdf:hasBroaderAuthority";
	public static final String HAS_EARLIER_ESTABLISHED_FORM = "madsrdf:hasEarlierEstablishedForm";
	public static final String HAS_LATER_ESTABLISHED_FORM = "madsrdf:hasLaterEstablishedForm";
	public static final String HAS_NARROWER_AUTHORITY = "madsrdf:hasNarrowerAuthority";
	public static final String HAS_RELATED_AUTHORITY = "madsrdf:hasRelatedAuthority";

	public static final List<String> RELATIONSHIP_KEYS = List.of(
			HAS_BROADER_AUTHORITY,
			HAS_EARLIER_ESTABLISHED_FORM,
			HAS_LATER_ESTABLISHED_FORM,
			HAS_NARROWER_AUTHORITY,
			HAS_RELATED_AUTHORITY
	);

	public static final String ASSOCIATED_LOCALE = "madsrdf:associatedLocale";
	public static final String BIRTH_PLACE = "madsrdf:birthPlace";
	public static final String DEATH_PLACE = "madsrdf:deathPlace";
	public static final String FIELD_OF_ACTIVITY = "madsrdf:fieldOfActivity";
	public static final String HAS_AFFILIATION = "madsrdf:hasAffiliation";
	public static final String OCCUPATION = "madsrdf:occupation";
	public static final Map<String, String> RDA_FIELDS = Map.of(
			ASSOCIATED_LOCALE, "Country",
			FIELD_OF_ACTIVITY, "Field",
			HAS_AFFILIATION, "Group/Organization",
			OCCUPATION, "Occupation"
	);

	private MadsConstants() {}
}
