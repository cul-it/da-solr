package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.utilities.CharacterSetUtils.hasCJK;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.isCJK;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrField;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrFields;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadType;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.DataField.FieldValues;
import edu.cornell.library.integration.marc.DataFieldSet;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.utilities.NameUtils;

/**
 * process the whole 7xx range into a wide variety of fields
 *
 */
public class TitleChange implements ResultSetToFields {

	static ObjectMapper mapper = new ObjectMapper();

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.addDataFieldResultSet( results.get("added_entry") );
		rec.addDataFieldResultSet( results.get("linking_entry") );

		Map<String,SolrInputField> fields = new HashMap<>();
		SolrFields vals = generateSolrFields( rec, config );
		for ( SolrField f : vals.fields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);		
		return fields;
	}

	public static SolrFields generateSolrFields( MarcRecord rec, SolrBuildConfig config )
			throws ClassNotFoundException, SQLException, IOException {
		Collection<DataFieldSet> sets = rec.matchAndSortDataFields();

		SolrFields sfs = new SolrFields();
		for( DataFieldSet fs: sets ) {

			if (  Character.getNumericValue(fs.getMainTag().charAt(1)) > 5 )
				sfs.addAll(processLinkingTitleFields(fs));
			else if (  Character.getNumericValue(fs.getMainTag().charAt(1)) <= 2 )
				sfs.addAll(processAuthorAddedEntryFields(config,fs));
			else if ( fs.getMainTag().equals("730") || fs.getMainTag().equals("740"))
				sfs.addAll(processTitleAddedEntryFields(fs));
			else
				System.out.println("Unrecognized field tag: "+fs.getFields().get(0).toString());
		}
		return sfs;
	}

	private static List<SolrField> processTitleAddedEntryFields(DataFieldSet fs) {
		List<SolrField> sfs = new ArrayList<>();
		for (DataField f : fs.getFields()) {
			String workField, title_cts, relation;
			if (f.mainTag.equals("730")) {
				title_cts = f.concatenateSubfieldsOtherThan("6i");
				workField = f.concatenateSpecificSubfields("iaplskfmnordgh");
				final String searchField = f.concatenateSpecificSubfields("abcdefghjklmnopqrstuvwxyz");
				sfs.add(new SolrField("title_uniform_t",searchField));
			} else {
				title_cts = f.concatenateSpecificSubfields("ab");
				workField = f.concatenateSpecificSubfields("iabchqdeklxftgjmnoprsuvwyz");
			}
			if (f.ind2.equals('2'))
				relation = "included_work_display";
			else
				relation = "related_work_display";

			sfs.add(new SolrField( relation, workField+'|'+title_cts));

		}
		return sfs;
	}

	private static List<SolrField> processAuthorAddedEntryFields(SolrBuildConfig config, DataFieldSet fs)
			throws ClassNotFoundException, SQLException, IOException {
		List<FieldValues> ctsValsList  = NameUtils.authorAndOrTitleValues(fs);
		if (ctsValsList == null) return null;

		// Check for special case - exactly two matching AUTHOR entries
		List<DataField> fields = fs.getFields();
		if ( fields.size() == 2
				&& ctsValsList.get(0).type.equals(HeadType.AUTHOR)
				&& ctsValsList.get(0).type.equals(HeadType.AUTHOR)
				&& fields.get(0).mainTag.equals(fields.get(1).mainTag)) {
			return NameUtils.combinedRomanNonRomanAuthorEntry( config, fs, ctsValsList, false );
		}

		// In all other cases, process the fields in the set individually
		List<SolrField> sfs = new ArrayList<>();
		for ( int i = 0 ; i < fs.getFields().size() ; i++ ) {
			DataField f = fs.getFields().get(i);
			FieldValues ctsVals = ctsValsList.get(i);
			sfs.addAll( NameUtils.singleAuthorEntry(config, f, ctsVals, false) );
		}
		return sfs;

	}

	private static List<SolrField> processLinkingTitleFields(DataFieldSet fs) {

		String relation = null;
		MAIN: switch (fs.getMainTag()) {
		case "780":
			switch (fs.getFields().get(0).ind2) {
			case '0': relation = "continues";			break MAIN;
			case '1': relation = "continues_in_part";	break MAIN;
			case '2':
			case '3': relation = "supersedes";			break MAIN;
			case '4': relation = "merger_of";			break MAIN;
			case '5': relation = "absorbed";			break MAIN;
			case '6': relation = "absorbed_in_part";	break MAIN;
			case '7': relation = "separated_from";		break MAIN;
			default:  return null;
			}
		case "785":
			switch (fs.getFields().get(0).ind2) {
			case '0': relation = "continued_by";		break MAIN;
			case '1': relation = "continued_in_part_by";break MAIN;
			case '2':
			case '3': relation = "superseded_by";		break MAIN;
			case '4': relation = "absorbed_by";			break MAIN;
			case '5': relation = "absorbed_in_part_by";	break MAIN;
			case '6': relation = "split_into";			break MAIN;
			case '7': relation = "merger";				break MAIN;
			default:  return null;
			}
		case "765": relation = "translation_of";	break MAIN;
		case "767": relation = "has_translation";	break MAIN;
		case "775": relation = "other_edition";		break MAIN;
		case "770": relation = "has_supplement";	break MAIN;
		case "772": relation = "supplement_to";		break MAIN;
		case "776": relation = "other_form";		break MAIN;
		case "777": relation = "issued_with";		break MAIN;
		default:    return null;
		}

		List<SolrField> sfs = new ArrayList<>();
		for (DataField f : fs.getFields()) {
			FieldValues authorTitle = f.getFieldValuesForNameAndOrTitleField("abcdgknqrst");
			sfs.add(new SolrField("title_uniform_t",authorTitle.title));
			if (f.tag.equals("880")) {
				if (f.getScript().equals(DataField.Script.CJK))
					sfs.add(new SolrField("title_uniform_t_cjk",authorTitle.title));
				else
					if (hasCJK(authorTitle.title))
						sfs.add(new SolrField("title_uniform_t_cjk",authorTitle.title));
			} else {
				if (isCJK(authorTitle.title))
					sfs.add(new SolrField("title_uniform_t_cjk",authorTitle.title));
			}
			// display no longer dependent on ind1 = '0'.
			StringBuilder sb = new StringBuilder();
			sb.append( f.concatenateSpecificSubfields("i") ).append(' ');
			if (authorTitle.author != null)
				sb.append(authorTitle.author).append(" | ");
			sb.append(authorTitle.title);
			sfs.add(new SolrField(relation+"_display",sb.toString())); // cts to re-add when applicable
		}
		return sfs;
	}

}
