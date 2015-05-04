package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.utilities.FilingNormalization.getSortHeading;
import static edu.cornell.library.integration.indexer.utilities.IndexingUtilities.removeTrailingPunctuation;

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

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
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
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {
		
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
			Set<String> subdivisionsGeo = new HashSet<String>();
			Set<String> subdivisionsEra = new HashSet<String>();
			Set<String> subdivisionsGenre = new HashSet<String>();
			boolean isWork = false;
		
			String main_fields = "", dashed_fields = "", facet_type = "topic";
			for (DataField f: dataFields) {
				
				switch (f.mainTag) {
				case "600":
					main_fields = "abcdefghkjlmnopqrstu";
					dashed_fields = "vxyz";
					isWork = isWork(f);
					break;
				case "610":
					main_fields = "abcdefghklmnoprstu";
					dashed_fields = "vxyz";
					isWork = isWork(f);
					break;
				case "611":
					main_fields = "acdefghklnpqstu";
					dashed_fields = "vxyz";
					isWork = isWork(f);
					break;
				case "630":
					main_fields = "adfghklmnoprst";
					dashed_fields = "vxyz";
					break;
				case "648":
					main_fields = "a";
					dashed_fields = "vxyz";
					facet_type = "era";
					break;
				case "650":
					main_fields = "abcd";
					dashed_fields = "vxyz";
					break;
				case "651":
					main_fields = "a";
					dashed_fields = "vxyz";
					facet_type = "geo";
					break;
				case "653":
					// This field list is used for subject_display and sixfivethree.
					main_fields = "a";
					break;
				case "654":
					main_fields = "abe";
					dashed_fields = "vyz";
					break;
				case "655":
					main_fields = "ab"; //655 facet_type over-riden for FAST facet
					dashed_fields = "vxyz";
					break;
				case "656":
					main_fields = "ak";
					dashed_fields = "vxyz";
					break;
				case "657":
					main_fields = "a";
					dashed_fields = "vxyz";
					break;
				case "658":
					main_fields = "abcd";
					break;
				case "662":
					main_fields = "abcdfgh";
					facet_type = "geo";
					break;
				case "690":
				case "691":
					main_fields = "abvxyz";
					break;
				case "692":
				case "693":
				case "694":
				case "695":
				case "696":
				case "697":
				case "698":
				case "699":
					main_fields = "a";
					break;
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
					// look for subdivisions to add to facets
					String subdivision = f.concatenateSpecificSubfields("v");
					if ( ! subdivision.isEmpty() )
						subdivisionsGenre.add(subdivision);
					subdivision = f.concatenateSpecificSubfields("y");
					if ( ! subdivision.isEmpty() )
						subdivisionsEra.add(subdivision);
					subdivision = f.concatenateSpecificSubfields("z");
					if ( ! subdivision.isEmpty() )
						subdivisionsGeo.add(subdivision);
				}
			}

			
			for (String s: values880_breadcrumbed) {
				addField(solrFields,"subject_"+facet_type+"_facet",removeTrailingPunctuation(s,"."));
				addField(solrFields,(isWork?"authortitle_":"subject_")+h.mainTag+"_filing",getSortHeading(s));
				addField(solrFields,"subject_addl_t",s);
				if (h.isFAST)
					if (h.mainTag.equals("655"))
						addField(solrFields,"fast_genre_facet",removeTrailingPunctuation(s,"."));
					else
						addField(solrFields,"fast_"+facet_type+"_facet",removeTrailingPunctuation(s,"."));
				if ( ! h.isFAST || ! recordHasLCSH)
					addField(solrFields,"subject_display",removeTrailingPunctuation(s,"."));
			}
			for (String s: valuesMain_breadcrumbed) {
				addField(solrFields,"subject_"+facet_type+"_facet",removeTrailingPunctuation(s,"."));
				addField(solrFields,(isWork?"authortitle_":"subject_")+h.mainTag+"_filing",getSortHeading(s));
				addField(solrFields,"subject_addl_t",s);
				if (h.isFAST)
					if (h.mainTag.equals("655"))
						addField(solrFields,"fast_genre_facet",removeTrailingPunctuation(s,"."));
					else
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
			for (String s: subdivisionsGenre)
				addField(solrFields,"subject_topic_facet",removeTrailingPunctuation(s,"."));
			for (String s: subdivisionsGeo)
				addField(solrFields,"subject_geo_facet",removeTrailingPunctuation(s,"."));
			for (String s: subdivisionsEra)
				addField(solrFields,"subject_era_facet",removeTrailingPunctuation(s,"."));
		
		}
		
		SolrInputField field = new SolrInputField("fast_b");
		field.setValue(recordHasFAST, 1.0f);
		solrFields.put("fast_b", field);
		
		return solrFields;
	}
	
	
	private boolean isWork(DataField f) {
		f.concatenateSpecificSubfields("tklfnpmors");
		for (Subfield sf : f.subfields.values())
			switch (sf.code) {
			case 't': case 'k': case 'l': case 'f': case 'n':
			case 'p': case 'm': case 'o': case 'r': case 's':
				return true;
			}
		return false;
	}


	private class Heading {
		boolean isFAST = false;
		FieldSet fs = null;
		String mainTag = null;
	}

}
