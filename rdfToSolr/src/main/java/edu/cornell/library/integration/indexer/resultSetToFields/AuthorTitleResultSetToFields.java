package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.utilities.IndexingUtilities.removeTrailingPunctuation;
import static edu.cornell.library.integration.indexer.utilities.FilingNormalization.getSortHeading;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;
import edu.cornell.library.integration.indexer.MarcRecord.Subfield;

/**
 * processing main entry result set into fields author_display
 * This could theoretically result in multiple author_display values, which would
 * cause an error submitting to Solr. This can only happen in the case of catalog
 * error, but a post-processor will remove extra values before submission leading
 * to a successful submission.
 */
public class AuthorTitleResultSetToFields implements ResultSetToFields {

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> solrFields = new HashMap<String,SolrInputField>();

		MarcRecord rec = new MarcRecord();

		rec.addDataFieldResultSet(results.get("main_entry"));
		rec.addDataFieldResultSet(results.get("title"),"245");
		rec.addDataFieldResultSet(results.get("title_240"),"240");
		Map<Integer,FieldSet> sortedFields = rec.matchAndSortDataFields();
		
		
		DataField title = null, title_vern = null, uniform_title = null, uniform_title_vern = null;
		String author = null, author_vern = null;
		// For each field and/of field group, add to SolrInputFields in precedence (field id) order,
		// but with organization determined by vernMode.
		Integer[] ids = sortedFields.keySet().toArray( new Integer[ sortedFields.keySet().size() ]);
		Arrays.sort( ids );
		for( Integer id: ids) {
			FieldSet fs = sortedFields.get(id);
			DataField[] dataFields = fs.fields.toArray( new DataField[ fs.fields.size() ]);
			Set<String> values880 = new HashSet<String>();
			Set<String> valuesMain = new HashSet<String>();
			Set<String> valuesFacet = new HashSet<String>();
			String dates = "";
			String cts = "";
			String cts880 = "";
			String mainTag = null;
			String filing_type = null;
		
			for (DataField f: dataFields) {
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
				} else {
					String subfields;
					String ctsSubfields;
					String facetOrFileSubfields;
					if (mainTag.equals("100")) {
						subfields = "abcq";
						ctsSubfields = "abcdq";
						facetOrFileSubfields = "abcdq";
						filing_type = "pers";
					} else {
						subfields = "abcdefghijklmnopqrstuvwxyz";
						ctsSubfields = "ab";
						facetOrFileSubfields = "abcdfghijklmnopqrstuvwxyz";
						filing_type = (mainTag.equals("110")) ? "corp" : "event";
					}
					String value = f.concatenateSpecificSubfields(subfields);
					if ( ! value.isEmpty() ) {
						if (f.tag.equals("880")) {
							values880.add(value);
							cts880 = f.concatenateSpecificSubfields(ctsSubfields);
						} else {
							if (mainTag.equals("100"))
								dates = removeTrailingPunctuation(f.concatenateSpecificSubfields("d"),".,");
							valuesMain.add(value);
							cts = f.concatenateSpecificSubfields(ctsSubfields);
						}
					}
					value = f.concatenateSpecificSubfields(facetOrFileSubfields);
					if ( ! value.isEmpty() )
						valuesFacet.add(value);
				}
			}
			if ((values880.size() == 1) && (valuesMain.size() == 1 )) {
				for (String s: values880)
					for (String t: valuesMain) {
						StringBuilder sb_piped = new StringBuilder();
						StringBuilder sb_disp = new StringBuilder();
						String cleaned_s = removeTrailingPunctuation(s,".,");
						sb_piped.append(cleaned_s);
						sb_disp.append(cleaned_s);
						sb_piped.append("|");
						sb_piped.append(cts880);
						sb_piped.append("|");
						sb_disp.append(" / ");
						sb_piped.append(t);
						sb_disp.append(t);
						if (! dates.isEmpty()) {
							sb_piped.append(" ");
							sb_piped.append(dates);
							sb_disp.append(" ");
							sb_disp.append(dates);
						}
						sb_piped.append("|");
						sb_piped.append(cts);
						String author_display = sb_disp.toString();
						addField(solrFields,"author_display",removeTrailingPunctuation(author_display,", "));
						addField(solrFields,"author_cts",sb_piped.toString());
						if (dates.isEmpty()) {
							author_vern = s;
							author = t;
						} else {
							author_vern = s+" "+dates;
							author = t+" "+dates;
						}
					}
			} else {
				for (String s: values880)
					if (dates.isEmpty()) {
						addField(solrFields,"author_cts",s+"|"+cts880);
						addField(solrFields,"author_display",removeTrailingPunctuation(s,", "));
						author_vern = s;
					} else {
						addField(solrFields,"author_cts",s+" "+dates+"|"+cts880);
						addField(solrFields,"author_display",removeTrailingPunctuation(s+" "+dates,", "));
						author_vern = s+" "+dates;
					}
				for (String s: valuesMain)
					if (dates.isEmpty()) {
						addField(solrFields,"author_cts",s+"|"+cts);
						addField(solrFields,"author_display",removeTrailingPunctuation(s,", "));
						author = s;
					} else {
						addField(solrFields,"author_cts",s+" "+dates+"|"+cts);
						addField(solrFields,"author_display",removeTrailingPunctuation(s+" "+dates,", "));
						author = s+" "+dates;
					}
			}
			for (String s : valuesFacet) {
				String sort = getSortHeading(s);
				addField(solrFields,"author_"+filing_type+"_filing",sort);
				addField(solrFields,"author_facet",removeTrailingPunctuation(s,"., "));
			}
				
			if (valuesMain.size() > 0) {
				String sort_author = getSortHeading(valuesMain.iterator().next());
				if (! dates.isEmpty())
					sort_author += " " + dates;
				addField(solrFields,"author_sort",sort_author);
			}
		}
		if (uniform_title != null) {

			if (uniform_title_vern != null) {

				String verntitle = removeTrailingPunctuation(uniform_title_vern.concatenateSpecificSubfields("adfgklmnoprs"),".,");
				String browsetitle = removeTrailingPunctuation(uniform_title_vern.concatenateSpecificSubfields("adgklmnoprs"),".,");
				if (author_vern != null) {
					String uniform_vern_cts = verntitle+"|"+verntitle+"|"+author_vern;
					addField(solrFields,"title_uniform_display",uniform_vern_cts);
					String browse = author_vern+" "+browsetitle;
					addField(solrFields,"authortitle_facet",browse);
					addField(solrFields,"authortitle_filing",getSortHeading(browse));
				} else if (author != null) {
					String uniform_vern_cts = verntitle+"|"+verntitle+"|"+author;
					addField(solrFields,"title_uniform_display",uniform_vern_cts);
					String browse = author+" "+browsetitle;
					addField(solrFields,"authortitle_facet",browse);
					addField(solrFields,"authortitle_filing",getSortHeading(browse));
				}
			}

			String browsetitle = removeTrailingPunctuation(uniform_title.concatenateSpecificSubfields("adgklmnoprs"),".,");
			String fulltitle = removeTrailingPunctuation(uniform_title.concatenateSpecificSubfields("adfgklmnoprs"),".,");
			if (author != null) {
				String uniform_cts = fulltitle+"|"+fulltitle+"|"+author;
				addField(solrFields,"title_uniform_display",uniform_cts);
				String browse = author+" "+browsetitle;
				addField(solrFields,"authortitle_facet",browse);
				addField(solrFields,"authortitle_filing",getSortHeading(browse));
			}
		}
		String responsibility = null, responsibility_vern = null;
		if (title != null) {
		
			// main title display fields
			for (Subfield sf : title.subfields.values())
				if (sf.code.equals('h'))
					sf.value = sf.value.replaceAll("\\[.*\\]", "");
			String maintitle = removeTrailingPunctuation(title.concatenateSpecificSubfields("a"),".,;:：/／= ");
			addField(solrFields,"title_display",maintitle);
			addField(solrFields,"subtitle_display",
					removeTrailingPunctuation(title.concatenateSpecificSubfields("bdefgknpqsv"),".,;:：/／= "));
			String fulltitle = removeTrailingPunctuation(title.concatenateSpecificSubfields("abdefghknpqsv"),".,;:：/／= ");
			addField(solrFields,"fulltitle_display",fulltitle);
			responsibility = title.concatenateSpecificSubfields("c");

			// sort title
			String sortTitle = fulltitle;
			if (Character.isDigit(title.ind2)) {
				int nonFilingCharCount = Character.digit(title.ind2, 10);
				if (nonFilingCharCount < sortTitle.length())
					sortTitle = sortTitle.substring(nonFilingCharCount);
			}
			sortTitle = getSortHeading(sortTitle);
			addField(solrFields,"title_sort",sortTitle);
	
			// title alpha buckets
			String alpha1Title = sortTitle.replaceAll("\\W", "").replaceAll("[^a-z]", "1");
			switch (Math.min(2,alpha1Title.length())) {
			case 2:
				addField(solrFields,"title_2letter_s",alpha1Title.substring(0,2));
				//NO break intended
			case 1:
				addField(solrFields,"title_1letter_s",alpha1Title.substring(0,1));
				break;
			case 0: break;
			default:
				System.out.println("The min of (2,length()) cannot be anything other than 0, 1, 2.");
				System.exit(1);
			}

			if ( (author != null) && ( uniform_title == null) ) {
				String authorTitle = author + " " + maintitle;
				addField(solrFields,"authortitle_facet",authorTitle);
				addField(solrFields,"authortitle_filing",getSortHeading(authorTitle));
			}
		}
		if (title_vern != null) {
			for (Subfield sf : title_vern.subfields.values())
				if (sf.code.equals('h'))
					sf.value = sf.value.replaceAll("\\[.*\\]", "");
			String maintitle_vern = removeTrailingPunctuation(title_vern.concatenateSpecificSubfields("a"),".,;:：/／= ");
			addField(solrFields,"title_vern_display",maintitle_vern);
			addField(solrFields,"subtitle_vern_display",
					removeTrailingPunctuation(title_vern.concatenateSpecificSubfields("bdefgknpqsv"),".,;:：/／= "));
			String fulltitle_vern = removeTrailingPunctuation(title_vern.concatenateSpecificSubfields("abdefghknpqsv"),".,;:：/／= ");
			addField(solrFields,"fulltitle_vern_display",fulltitle_vern);
			responsibility_vern = title_vern.concatenateSpecificSubfields("c");

			if (uniform_title_vern == null) {
				if (author_vern != null) {
					String authorTitle = author_vern + " " + maintitle_vern;
					addField(solrFields,"authortitle_facet",authorTitle);
					addField(solrFields,"authortitle_filing",getSortHeading(authorTitle));
				} else if (author != null) {
					String authorTitle = author + " " + maintitle_vern;
					addField(solrFields,"authortitle_facet",authorTitle);
					addField(solrFields,"authortitle_filing",getSortHeading(authorTitle));
				}
			}
		}
		if (responsibility != null && ! responsibility.isEmpty()) {
			if (responsibility_vern != null && ! responsibility_vern.isEmpty())
				addField(solrFields,"title_responsibility_display",
						responsibility_vern + " / " + responsibility);
			else 
				addField(solrFields,"title_responsibility_display", responsibility);
		}
		return solrFields;
	}	

}
