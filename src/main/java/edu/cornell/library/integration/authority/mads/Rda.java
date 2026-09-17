package edu.cornell.library.integration.authority.mads;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.apicatalog.jsonld.JsonLdError;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static edu.cornell.library.integration.authority.mads.Constants.ASSOCIATED_LOCALE;
import static edu.cornell.library.integration.authority.mads.Constants.BIRTH_PLACE;
import static edu.cornell.library.integration.authority.mads.Constants.DEATH_PLACE;
import static edu.cornell.library.integration.authority.mads.Constants.FIELD_OF_ACTIVITY;
import static edu.cornell.library.integration.authority.mads.Constants.HAS_AFFILIATION;
import static edu.cornell.library.integration.authority.mads.Constants.OCCUPATION;
import jakarta.json.JsonObject;
import jakarta.json.JsonValue;

public class Rda {
	public static final Map<String, String> RDA_FIELDS = Map.of(
			ASSOCIATED_LOCALE, "Country",
			BIRTH_PLACE, "Birth Place",
			DEATH_PLACE, "Place of Death",
			FIELD_OF_ACTIVITY, "Field",
			HAS_AFFILIATION, "Group/Organization",
			OCCUPATION, "Occupation"
	);
	static final ObjectMapper mapper = new ObjectMapper();

	public static Map<String,Collection<String>> rda(Connection authority, AuthorityData rec) throws SQLException, JsonLdError, IOException, URISyntaxException, InterruptedException {
			Map<String,Collection<String>> data = new HashMap<>();
			JsonValue rwosVal = rec.mainEntry.get("madsrdf:identifiesRWO");
			List<String> rwoIds = JsonldUtils.getIdAsList(rwosVal);
			for (var rwoId : rwoIds) {
				JsonObject rwoJson = JsonldUtils.getJsonObjectForId(rec.graph, rwoId);
				if (rwoJson.isEmpty()) continue;

				RWO rwo = RWO.parse(authority, rwoJson, rec.graph, rwoId);
				// RWO rwo = RWO.fromJsonObject(authority, rwoJson, rwoId);
				add(data, RDA_FIELDS.get(ASSOCIATED_LOCALE), rwo.associatedLocale());
				add(data, RDA_FIELDS.get(BIRTH_PLACE), rwo.birthPlace());
				add(data, RDA_FIELDS.get(DEATH_PLACE), rwo.deathPlace());
				add(data, RDA_FIELDS.get(FIELD_OF_ACTIVITY), rwo.fieldOfActivities());
				add(data, RDA_FIELDS.get(HAS_AFFILIATION), rwo.hasAffiliations());
				add(data, RDA_FIELDS.get(OCCUPATION), rwo.occupation());
			}

			return data;
		}

		protected static void add(Map<String,Collection<String>> data, String field, List<String> values) {
			if ( ! data.containsKey(field))
				data.put(field, new HashSet<>());
			for (var value : values)
				data.get(field).add(value);
		}

		public static String json(Map<String,Collection<String>> data) throws JsonProcessingException {
			if (data.isEmpty()) return null;
			return mapper.writeValueAsString(data);
		}
}
