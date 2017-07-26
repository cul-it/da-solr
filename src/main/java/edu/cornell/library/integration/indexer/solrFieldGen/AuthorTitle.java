package edu.cornell.library.integration.indexer.solrFieldGen;

import static edu.cornell.library.integration.utilities.CharacterSetUtils.hasCJK;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.limitStringToGSMChars;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.standardizeApostrophes;
import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;
import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.JenaResultsToMarcRecord;
import edu.cornell.library.integration.indexer.utilities.SolrFields;
import edu.cornell.library.integration.indexer.utilities.SolrFields.BooleanSolrField;
import edu.cornell.library.integration.indexer.utilities.SolrFields.SolrField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.DataFieldSet;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.utilities.CharacterSetUtils;
import edu.cornell.library.integration.utilities.FieldValues;
import edu.cornell.library.integration.utilities.NameUtils;

/**
 * processing main entry result set into fields author_display
 * This could theoretically result in multiple author_display values, which would
 * cause an error submitting to Solr. This can only happen in the case of catalog
 * error, but a post-processor will remove extra values before submission leading
 * to a successful submission.
 */
public class AuthorTitle implements ResultSetToFields, SolrFieldGenerator {

	static ObjectMapper mapper = new ObjectMapper();

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		JenaResultsToMarcRecord.addDataFieldResultSet( rec, results.get("title") );
		JenaResultsToMarcRecord.addDataFieldResultSet( rec, results.get("title_240") );
		JenaResultsToMarcRecord.addDataFieldResultSet( rec, results.get("main_entry") );

		Map<String,SolrInputField> fields = new HashMap<>();
		SolrFields vals = generateSolrFields( rec, config );
		for ( SolrField f : vals.fields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);
		for ( BooleanSolrField f : vals.boolFields ) {
			SolrInputField boolField = new SolrInputField(f.fieldName);
			boolField.setValue(f.fieldValue, 1.0f);
			fields.put(f.fieldName, boolField);
		}
		return fields;
	}

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	public List<String> getHandledFields() {
		return Arrays.asList("100","110","111","240","245");
	}

	@Override
	// This field generator uses currently untracked authority data, so should be regenerated more often.
	public Duration resultsShelfLife() { return Duration.ofDays(14); }

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, SolrBuildConfig config )
			throws ClassNotFoundException, SQLException, IOException {

		List<SolrField> sfs = new ArrayList<>();
		DataField title = null, title_vern = null, uniform_title = null, uniform_title_vern = null;
		String author = null, author_vern = null;

		// Encourage linking of author fields where useful
		int authorFields = 0;
		for (DataField f : rec.dataFields) if (f.mainTag.startsWith("1")) authorFields++;
		if (authorFields == 2)
			for (DataField f : rec.dataFields) if (f.mainTag.startsWith("1")) f.linkNumber = -23;

		Collection<DataFieldSet> sets = rec.matchAndSortDataFields();
		int authorDisplayCount = 0;
		for( DataFieldSet fs: sets ) {

			String mainTag = null;
			
			if (fs.getMainTag().startsWith("1")) {
				List<FieldValues> ctsValsList  = NameUtils.authorAndOrTitleValues(fs);
				
				// Check for special case - exactly two matching author entries
				List<DataField> fields = fs.getFields();
				if ( fields.size() == 2
						&& fields.get(0).mainTag.equals(fields.get(1).mainTag)) {
					sfs.addAll( NameUtils.combinedRomanNonRomanAuthorEntry( config, fs, ctsValsList, true ) );
					author_vern = ctsValsList.get(0).author;
					author = ctsValsList.get(1).author;
					authorDisplayCount++;
		
					// In other cases, process the fields individually
				} else {
					for ( int i = 0 ; i < fs.getFields().size() ; i++ ) {
						DataField f = fs.getFields().get(i);
						FieldValues ctsVals = ctsValsList.get(i);
						sfs.addAll( NameUtils.singleAuthorEntry(config, f, ctsVals, true) );
						if (f.tag.equals("880"))
							author_vern = ctsVals.author;
						else
							author = ctsVals.author;
						authorDisplayCount++;
					}
				}
			}
		
			for (DataField f: fs.getFields()) {
				mainTag = f.mainTag;
				if (mainTag.equals("245")) {
					if (f.tag.equals("245"))
						title = f;
					else
						title_vern = f;
				} else if (mainTag.equals("240")) {
					if (f.tag.equals("240"))
						uniform_title = f;
					else
						uniform_title_vern = f;
				}
			}

		}
		if (authorDisplayCount > 1) {
			System.out.println( "Record "+rec.id+" has erroneous main entry author fields.");
			sfs = mergeAuthorDisplayValues( sfs );
		}

		if (author != null) {
			String sort_author = getFilingForm(author);
			sfs.add(new SolrField("author_sort",sort_author));
		}
		if (uniform_title != null) {

			if (uniform_title_vern != null) {

				String verntitle = removeTrailingPunctuation(uniform_title_vern.concatenateSpecificSubfields("adfgklmnoprs"),".,/ ");
				String browsetitle = removeTrailingPunctuation(
						uniform_title_vern.getStringWithoutInitialArticle(uniform_title_vern.concatenateSpecificSubfields("adgklmnoprs")),
						".,/ ");
				if (author_vern != null) {
					String uniform_vern_cts = verntitle+"|"+verntitle+"|"+author_vern;
					sfs.add(new SolrField("title_uniform_display",uniform_vern_cts));
					String browse = author_vern+" | "+browsetitle;
					sfs.add(new SolrField("authortitle_facet",browse));
					sfs.add(new SolrField("authortitle_filing",getFilingForm(browse)));
				} else if (author != null) {
					String uniform_vern_cts = verntitle+"|"+verntitle+"|"+author;
					sfs.add(new SolrField("title_uniform_display",uniform_vern_cts));
					String browse = author+" | "+browsetitle;
					sfs.add(new SolrField("authortitle_facet",browse));
					sfs.add(new SolrField("authortitle_filing",getFilingForm(browse)));
				}
				String titleWOarticle = uniform_title_vern.getStringWithoutInitialArticle(verntitle);
				if (uniform_title_vern.getScript().equals(DataField.Script.CJK))
					sfs.add(new SolrField("title_uniform_t_cjk",verntitle));
				else {
					if (hasCJK(verntitle))
						sfs.add(new SolrField("title_uniform_t_cjk",verntitle));
					sfs.add(new SolrField("title_uniform_t",verntitle));
					sfs.add(new SolrField("title_uniform_t",titleWOarticle));
				}
			}

			String browsetitle = removeTrailingPunctuation(
					uniform_title.getStringWithoutInitialArticle(uniform_title.concatenateSpecificSubfields("adgklmnoprs")),".,/ ");
			String fulltitle = removeTrailingPunctuation(uniform_title.concatenateSpecificSubfields("adfgklmnoprs"),".,/ ");

			if (author != null) {
				String uniform_cts = fulltitle+"|"+fulltitle+"|"+author;
				sfs.add(new SolrField("title_uniform_display",uniform_cts));
				String browse = author+" | "+browsetitle;
				sfs.add(new SolrField("authortitle_facet",browse));
				sfs.add(new SolrField("authortitle_filing",getFilingForm(browse)));
			}
			sfs.add(new SolrField("title_uniform_t",fulltitle));
			sfs.add(new SolrField("title_uniform_t",uniform_title.getStringWithoutInitialArticle(fulltitle)));
		}
		String responsibility = null, responsibility_vern = null;
		if (title == null) {

			System.out.println("Bib record "+rec.id+" has no main title.");
		} else  {
		
			for (Subfield sf : title.subfields)
				if (sf.code.equals('h'))
					sf.value = sf.value.replaceAll("\\[.*\\]", "");

			String fulltitle = removeTrailingPunctuation(title.concatenateSpecificSubfields("abdefghknpqsv"),".,;:=/ ");

			// sort title
			String titleWOArticle = title.getStringWithoutInitialArticle(fulltitle);
			String sortTitle = getFilingForm(titleWOArticle);
			sfs.add(new SolrField("title_sort",sortTitle));

			// main title display fields
			String maintitle = removeTrailingPunctuation(title.concatenateSpecificSubfields("a"),".,;:=/ ");
			if (maintitle.isEmpty()) System.out.println("Bib record "+rec.id+" has no main title.");
			sfs.add(new SolrField("title_display",maintitle));
			sfs.add(new SolrField("subtitle_display",
					removeTrailingPunctuation(title.concatenateSpecificSubfields("bdefgknpqsv"),".,;:=/ ")));
			sfs.add(new SolrField("fulltitle_display",fulltitle));
			sfs.add(new SolrField("title_t",fulltitle));
			sfs.add(new SolrField("title_t",titleWOArticle));
			sfs.add(new SolrField("title_exact",standardizeApostrophes(fulltitle)));
			sfs.add(new SolrField("title_exact",standardizeApostrophes(titleWOArticle)));
			sfs.add(new SolrField("title_sms_compat_display",limitStringToGSMChars(maintitle)));
			responsibility = title.concatenateSpecificSubfields("c");

			// title alpha buckets
			String alpha1Title = sortTitle.replaceAll("\\W", "").replaceAll("[^a-z]", "1");
			switch (Math.min(2,alpha1Title.length())) {
			case 2:
				sfs.add(new SolrField("title_2letter_s",alpha1Title.substring(0,2)));
				//NO break intended
			case 1:
				sfs.add(new SolrField("title_1letter_s",alpha1Title.substring(0,1)));
				break;
			case 0: break;
			default:
				System.out.println("The min of (2,length()) cannot be anything other than 0, 1, 2.");
				System.exit(1);
			}

			if ( (author != null) && ( uniform_title == null) ) {
				String authorTitle = author + " | " + title.getStringWithoutInitialArticle(maintitle);
				sfs.add(new SolrField("authortitle_facet",authorTitle));
				sfs.add(new SolrField("authortitle_filing",getFilingForm(authorTitle)));
			}
		}
		if (title_vern != null) {
			for (Subfield sf : title_vern.subfields)
				if (sf.code.equals('h'))
					sf.value = sf.value.replaceAll("\\[.*\\]", "");
			String maintitle_vern = removeTrailingPunctuation(title_vern.concatenateSpecificSubfields("a"),".,;:=/\uFF0F ");
			sfs.add(new SolrField("title_vern_display",maintitle_vern));
			sfs.add(new SolrField("subtitle_vern_display",
					removeTrailingPunctuation(title_vern.concatenateSpecificSubfields("bdefgknpqsv"),".,;:=/\uFF0F ")));
			String fulltitle_vern = removeTrailingPunctuation(title_vern.concatenateSpecificSubfields("abdefghknpqsv"),".,;:=/\uFF0F ");
			String titleWOarticle = title_vern.getStringWithoutInitialArticle(fulltitle_vern);

			if (title_vern.getScript().equals(DataField.Script.CJK))
				sfs.add(new SolrField("title_t_cjk",fulltitle_vern));
			else {
				if (hasCJK(fulltitle_vern))
					sfs.add(new SolrField("title_t_cjk",fulltitle_vern));
				sfs.add(new SolrField("title_t",fulltitle_vern));
				sfs.add(new SolrField("title_t",titleWOarticle));
			}
			sfs.add(new SolrField("fulltitle_vern_display",fulltitle_vern));
			sfs.add(new SolrField("title_exact",fulltitle_vern));
			sfs.add(new SolrField("title_exact",titleWOarticle));

			responsibility_vern = title_vern.concatenateSpecificSubfields("c");

			if (uniform_title_vern == null) {
				if (author_vern != null) {
					String authorTitle = author_vern + " | " + title_vern.getStringWithoutInitialArticle(maintitle_vern);
					sfs.add(new SolrField("authortitle_facet",authorTitle));
					sfs.add(new SolrField("authortitle_filing",getFilingForm(authorTitle)));
				} else if (author != null) {
					String authorTitle = author + " | " + title_vern.getStringWithoutInitialArticle(maintitle_vern);
					sfs.add(new SolrField("authortitle_facet",authorTitle));
					sfs.add(new SolrField("authortitle_filing",getFilingForm(authorTitle)));
				}
			}
		}
		if (responsibility != null && ! responsibility.isEmpty()) {
			if (responsibility_vern != null && ! responsibility_vern.isEmpty()) {
				sfs.add(new SolrField("title_responsibility_display",
						responsibility_vern + " / " + responsibility));
				sfs.add(new SolrField(CharacterSetUtils.isCJK(responsibility_vern)
						?"author_245c_t_cjk":"author_245c_t", responsibility_vern));
			} else {
				sfs.add(new SolrField("title_responsibility_display", responsibility));
			}
			sfs.add(new SolrField("author_245c_t", responsibility));
		}
		SolrFields solrFields = new SolrFields();
		solrFields.fields = sfs;
		return solrFields;
	}

	private static List<SolrField> mergeAuthorDisplayValues(List<SolrField> sfs) {
		List<SolrField> newSfs = new ArrayList<>();
		String authorDisplayValue = null;
		for (SolrField sf : sfs)
			if (sf.fieldName.equals("author_display")) {
				if (authorDisplayValue == null)	authorDisplayValue = sf.fieldValue;
			} else newSfs.add(sf);
		if ( authorDisplayValue != null )
			newSfs.add(new SolrField("author_display",authorDisplayValue));
		return newSfs;
	}

}
