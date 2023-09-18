package edu.cornell.library.integration.metadata.generator;

import static edu.cornell.library.integration.utilities.CharacterSetUtils.hasCJK;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.isCJK;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.DataFieldSet;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.metadata.support.HeadingCategory;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.FieldValues;
import edu.cornell.library.integration.utilities.NameUtils;
import edu.cornell.library.integration.utilities.SolrFields;
import edu.cornell.library.integration.utilities.SolrFields.SolrField;

/**
 * process the whole 7xx range into a wide variety of fields
 *
 */
public class TitleChange implements SolrFieldGenerator {

	@Override
	public String getVersion() { return "1.6"; }

	@Override
	public List<String> getHandledFields() {
		return Arrays.asList("700","710","711","720","730","740",
				"760","762","765","767","770","772","773","774","775","776","777","780","785","786","787");
	}

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config config ) throws SQLException, IOException {
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
				final String searchField = f.concatenateSpecificSubfields("anp");
				sfs.add(new SolrField("title_addl_t",searchField));
			}
			if (f.ind2.equals('2'))
				relation = "included_work_display";
			else
				relation = "related_work_display";

			sfs.add(new SolrField( relation, workField+'|'+title_cts));

		}
		return sfs;
	}

	private static List<SolrField> processAuthorAddedEntryFields(Config config, DataFieldSet fs)
			throws SQLException, IOException {
		List<FieldValues> ctsValsList  = NameUtils.authorAndOrTitleValues(fs);
		if (ctsValsList == null) return null;

		// Check for special case - exactly two matching AUTHOR entries
		List<DataField> fields = fs.getFields();
		if ( fields.size() == 2
				&& ctsValsList.get(0).category.equals(HeadingCategory.AUTHOR)
				&& ctsValsList.get(1).category.equals(HeadingCategory.AUTHOR)
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
			FieldValues authorTitle = FieldValues.getFieldValuesForNameAndOrTitleField(f,"abcgknpqrst");
			if ( ! authorTitle.title.isEmpty() )
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
			if ( fs.getMainTag().equals("776") ) {
				String display = get776DisplayForm( f, authorTitle );
				if ( ! display.isEmpty() )
					sfs.add(new SolrField("other_form_display",display));
				continue;
			}
			StringBuilder sb = new StringBuilder();
			sb.append( f.concatenateSpecificSubfields("i") ).append(' ');
			if (authorTitle.author != null)
				sb.append(authorTitle.author).append(" | ");
			sb.append(authorTitle.title);
			sfs.add(new SolrField(relation+"_display",sb.toString())); // cts to re-add when applicable
		}
		return sfs;
	}

	private static String get776DisplayForm( DataField f, FieldValues at ) {
		StringBuilder sb = new StringBuilder();
		boolean atAdded = false;
		boolean lastSFIdentifier = false;
		for ( Subfield sf : f.subfields ) {
			switch (sf.code) {

			// author title fields "abdgknpqrst"
			case 'a': case 'b': case 'd': case 'g': case 'k': case 'n':
			case 'p': case 'q': case 'r': case 's': case 't':
				if ( ! atAdded ) {
					if (sb.length() > 0 ) sb.append(' ');
					if (at.author != null)
						sb.append(at.author).append(" | ");
					sb.append(at.title);
					atAdded = true;
					lastSFIdentifier = false;
				}
				break;

			// identifier fields (try to display only when vocabulary is identified)
			case 'x':
				if ( lastSFIdentifier ) sb.append(", ");
				else if (sb.length() > 0 ) sb.append(' ');
				sb.append("ISSN: ").append(sf.value);
				lastSFIdentifier = true;
				break;
			case 'z':
				if ( lastSFIdentifier ) sb.append(", ");
				else if (sb.length() > 0 ) sb.append(' ');
				sb.append("ISBN: ").append(sf.value);
				lastSFIdentifier = true;
				break;
			case 'o':
			case 'w':
				boolean display = false;
				int length = sb.length();
				if ( (length != 0 && sb.lastIndexOf(":") == length - 1) ||
					( length != 2 && (sb.lastIndexOf("no.") == length - 3 || sb.lastIndexOf("UPC") == length - 3)) ||
					( length != 3 && sb.lastIndexOf("code") == length - 4) ||
					( length != 5 && sb.lastIndexOf("number") == length - 6) ||
					( length != 9 && sb.lastIndexOf("Identifier") == length - 10) )
					display = true;
				else if (  sf.value.indexOf(')') != -1
						|| sf.value.indexOf(':') != -1 )
					display = true;
				if ( ! display )
					break;
				if ( lastSFIdentifier ) sb.append(", ");
				else if (sb.length() > 0 ) sb.append(' ');
				sb.append(sf.value);
				lastSFIdentifier = true;
				break;

			// Other fields to display
			case 'h':

			// qualifier fields
			case 'c':
			case 'i':
				if (sb.length() > 0 ) sb.append(' ');
				sb.append(sf.value);
				lastSFIdentifier = false;
				break;
			}
		}
		return sb.toString();
	}


}
