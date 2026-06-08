package edu.cornell.library.integration.authority;

import static edu.cornell.library.integration.authority.Solr.identifySearchFields;
import static edu.cornell.library.integration.authority.Solr.querySolrForMatchingBibs;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import edu.cornell.library.integration.authority.IdentifyCuratedFlipCandidates.Flip;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.utilities.Config;

public class IdentifyBibFlipCandidates {

	static ObjectMapper mapper = new ObjectMapper();
	static { mapper.enable(SerializationFeature.INDENT_OUTPUT); }

	public static boolean fromFlipList( Config config, List<Flip> flips, String filename )
			throws SolrServerException, IOException {
		BufferedWriter candidatesWriter = Files.newBufferedWriter(Paths.get(filename));
		candidatesWriter.append("[\n");
		boolean candidateWritten = false;
		try( Http2SolrClient solr = new Http2SolrClient
				.Builder(config.getBlacklightSolrUrl())
				.withBasicAuthCredentials(config.getSolrUser(),config.getSolrPassword()).build();) {

			for (Flip flip : flips) {
				Map<String,Object> autoFlip = lookForBeforeHeading( solr, flip );
				if (autoFlip != null) {
					if (candidateWritten)
						candidatesWriter.append(",\n");
					else
						candidateWritten = true;
					candidatesWriter.append(mapper.writeValueAsString(autoFlip));
				}
			}
		}
		if (candidateWritten) {
			candidatesWriter.append("]\n");
			candidatesWriter.flush();
			candidatesWriter.close();
		}
		return candidateWritten;
	}

	private static String headingOf(DataField f) {
		String heading = f.concatenateSpecificSubfields("abcdefghjklmnopqrstu");
		String dashed = f.concatenateSpecificSubfields(" > ", "xvyz");
		if ( ! dashed.isEmpty() ) heading = String.format("%s > %s", heading, dashed);
		return heading;
	}

	private static Map<String,Object> lookForBeforeHeading(
			Http2SolrClient solr, Flip flip) throws SolrServerException, IOException {
		List<String> searchFields = identifyBlacklightFields(flip);
		Map<String,Object> autoFlip = new HashMap<>();
		String heading = headingOf(flip.before);
		for (String field : searchFields) {
			List<List<String>> instances = querySolrForMatchingBibs( solr, field, heading );
			if (instances.isEmpty()) continue;
			autoFlip.put(field, instances);
		}
		if (autoFlip.isEmpty()) return null;
		autoFlip.put("oldHeading", flip.before);
		autoFlip.put("newHeading", flip.after);
		autoFlip.put("name", "ListBased");
		if (flip.authorityIdBefore != null && Objects.equals(flip.authorityIdAfter, flip.authorityIdBefore))
			autoFlip.put("authorityId", flip.authorityIdBefore);
		else {
			if (flip.authorityIdAfter != null)  autoFlip.put("authorityIdAfter",  flip.authorityIdAfter);
			if (flip.authorityIdBefore != null) autoFlip.put("authorityIdBefore", flip.authorityIdBefore);
		}
		return autoFlip;
	}


	private static List<String> identifyBlacklightFields(Flip flip) {

		HeadingType before_ht = HeadingType.byAuthField( flip.before.tag );
		HeadingType after_ht = HeadingType.byAuthField( flip.after.tag );

		boolean includeAuthorFields = authorHeadingTypes.contains(before_ht) && authorHeadingTypes.contains(after_ht);
		AuthoritySource vocab = nonFast(flip.vocabs);
		List<String> searchFields = identifySearchFields(
				before_ht, vocab, flip.vocabs.contains(AuthoritySource.FAST));
		if ( ! includeAuthorFields ) searchFields.removeIf(p -> p.contains("author"));
		searchFields.removeIf(p -> p.contains("_unk_"));
		return searchFields;
	}

	private static AuthoritySource nonFast(EnumSet<AuthoritySource> vocabs) {
		for (AuthoritySource vocab : vocabs)
			if ( ! vocab.equals(AuthoritySource.FAST))
				return vocab;
		return null;
	}

	private static EnumSet<HeadingType> authorHeadingTypes = EnumSet.of(HeadingType.PERS, HeadingType.CORP, HeadingType.MEETING);

}
