package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.getSortHeading;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.removeTrailingPunctuation;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;
import edu.cornell.library.integration.indexer.MarcRecord.Subfield;

/**
 * processing date result sets into fields pub_date, pub_date_sort, pub_date_display
 * 
 */
public class SubjectResultSetToFields implements ResultSetToFields {

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> solrFields = new HashMap<String,SolrInputField>();

		MarcRecord rec = new MarcRecord();

		for( String resultKey: results.keySet()){
			rec.addDataFieldResultSet(results.get(resultKey));
		}
		Map<Integer,FieldSet> sortedFields = rec.matchAndSortDataFields();
		boolean recordHasFAST = false;
		boolean recordHasLCSH = false;
		
		Collection<Heading> taggedFields = new LinkedHashSet<Heading>();
		
		// For each field and/of field group, add to SolrInputFields in precedence (field id) order,
		// but with organization determined by vernMode.
		Integer[] ids = sortedFields.keySet().toArray( new Integer[ sortedFields.keySet().size() ]);
		Arrays.sort( ids );
		for( Integer id: ids) {
			FieldSet fs = sortedFields.get(id);
			// First DataField in each FieldSet should be representative, so we'll examine that.
			Heading h = new Heading();
			DataField f = fs.fields.iterator().next();
			h.mainTag = f.mainTag;
			if (f.ind2.equals('7')) {
				for ( Subfield sf : f.subfields.values() )
					if (sf.code.equals('2') 
							&& (sf.value.equalsIgnoreCase("fast")
									|| sf.value.equalsIgnoreCase("fast/NIC")
									|| sf.value.equalsIgnoreCase("fast/NIC/NAC"))) {
						recordHasFAST = true;
						h.isFAST = true;
						
					}
			} else if (f.ind2.equals('0')) {
				recordHasLCSH = true;
			}
			h.fs = fs;
			taggedFields.add(h);
		}
		for( Heading h : taggedFields) {
			DataField[] dataFields = h.fs.fields.toArray( new DataField[ h.fs.fields.size() ]);
			Set<String> values880_piped = new HashSet<String>();
			Set<String> valuesMain_piped = new HashSet<String>();
			Set<String> values880_breadcrumbed = new HashSet<String>();
			Set<String> valuesMain_breadcrumbed = new HashSet<String>();
		
			String main_fields = "", dashed_fields = "", facet_type = "topic";
			for (DataField f: dataFields) {
				
				if (f.mainTag.equals("600")) {
					main_fields = "abcdefghkjlmnopqrstu";
					dashed_fields = "vxyz";
				} else if (f.mainTag.equals("610")) {
					main_fields = "abcdefghklmnoprstu";
					dashed_fields = "vxyz";
				} else if (f.mainTag.equals("611")) {
					main_fields = "acdefghklnpqstu";
					dashed_fields = "vxyz";
				} else if (f.mainTag.equals("630")) {
					main_fields = "adfghklmnoprst";
					dashed_fields = "vxyz";
				} else if (f.mainTag.equals("648")) {
					main_fields = "a";
					dashed_fields = "vxyz";
					facet_type = "era";
				} else if (f.mainTag.equals("650")) {
					main_fields = "abcd";
					dashed_fields = "vxyz";
				} else if (f.mainTag.equals("651")) {
					main_fields = "a";
					dashed_fields = "vxyz";
					facet_type = "geo";
				} else if (f.mainTag.equals("653")) {
					// This field list is used for subject_display and sixfivethree.
					main_fields = "a";
				} else if (f.mainTag.equals("654")) {
					main_fields = "abe";
					dashed_fields = "vyz";
				} else if (f.mainTag.equals("655")) {
					main_fields = "ab";
					dashed_fields = "vxyz";
				} else if (f.mainTag.equals("656")) {
					main_fields = "ak";
					dashed_fields = "vxyz";
				} else if (f.mainTag.equals("657")) {
					main_fields = "a";
					dashed_fields = "vxyz";
				} else if (f.mainTag.equals("658")) {
					main_fields = "abcd";
				} else if (f.mainTag.equals("662")) {
					main_fields = "abcdfgh";
				} else if (f.mainTag.equals("690")) {
					main_fields = "abvxyz";
				} else if (f.mainTag.equals("691")) {
					main_fields = "abvxyz";
				} else if ((f.mainTag.equals("692")) || (f.mainTag.equals("693")) ||
						(f.mainTag.equals("694")) || (f.mainTag.equals("695")) ||
						(f.mainTag.equals("696")) || (f.mainTag.equals("697")) ||
						(f.mainTag.equals("698")) || (f.mainTag.equals("699"))) {
					main_fields = "a";
				}
				if (! main_fields.equals("")) {
					StringBuilder sb_piped = new StringBuilder();
					StringBuilder sb_breadcrumbed = new StringBuilder();
					String mainFields = f.concatenateSpecificSubfields(main_fields);
					sb_piped.append(mainFields);
					sb_breadcrumbed.append(mainFields);
					String dashed_terms = f.concatenateSpecificSubfields("|",dashed_fields);
					if (f.mainTag.equals("653")) {
						addField(solrFields,"sixfivethree",sb_piped.toString());
					}
					if ((dashed_terms != null) && ! dashed_terms.equals("")) {
						sb_piped.append("|"+dashed_terms);
						sb_breadcrumbed.append(" > "+dashed_terms.replaceAll(Pattern.quote("|"), " > "));		
					}
					if (f.tag.equals("880")) {
						values880_piped.add(removeTrailingPunctuation(sb_piped.toString(),"."));
						values880_breadcrumbed.add(removeTrailingPunctuation(sb_breadcrumbed.toString(),"."));
					} else {
						valuesMain_piped.add(removeTrailingPunctuation(sb_piped.toString(),"."));
						valuesMain_breadcrumbed.add(removeTrailingPunctuation(sb_breadcrumbed.toString(),"."));
					}
				}
			}

			
			for (String s: values880_breadcrumbed) {
				addField(solrFields,"subject_"+facet_type+"_facet",removeTrailingPunctuation(s,"."));
				addField(solrFields,"subject_"+h.mainTag+"_exact",getSortHeading(s));
				addField(solrFields,"subject_addl_t",s);
				if (h.isFAST)
					addField(solrFields,"fast_"+facet_type+"_facet",removeTrailingPunctuation(s,"."));
				if ( ! h.isFAST || ! recordHasLCSH)
					addField(solrFields,"subject_display",removeTrailingPunctuation(s,"."));
			}
			for (String s: valuesMain_breadcrumbed) {
				addField(solrFields,"subject_"+facet_type+"_facet",removeTrailingPunctuation(s,"."));				
				addField(solrFields,"subject_"+h.mainTag+"_exact",getSortHeading(s));
				addField(solrFields,"subject_addl_t",s);
				if (h.isFAST)
					addField(solrFields,"fast_"+facet_type+"_facet",removeTrailingPunctuation(s,"."));
				if ( ! h.isFAST || ! recordHasLCSH)
					addField(solrFields,"subject_display",removeTrailingPunctuation(s,"."));
			}

			for (String s: values880_piped) {
				if ( ! h.isFAST || ! recordHasLCSH)
					addField(solrFields,"subject_cts",s);
			}
			for (String s: valuesMain_piped) {
				if ( ! h.isFAST || ! recordHasLCSH)
					addField(solrFields,"subject_cts",s);
			}
		}
		
		SolrInputField field = new SolrInputField("fast_b");
		field.setValue(recordHasFAST, 1.0f);
		solrFields.put("fast_b", field);
		
		return solrFields;
	}
	
	
	private class Heading {
		boolean isFAST = false;
		FieldSet fs = null;
		String mainTag = null;
	}

}
