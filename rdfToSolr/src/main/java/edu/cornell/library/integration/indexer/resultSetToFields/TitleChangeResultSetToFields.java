package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.ilcommons.util.CharacterSetUtils.hasCJK;
import static edu.cornell.library.integration.ilcommons.util.CharacterSetUtils.isCJK;
import static edu.cornell.library.integration.ilcommons.util.CharacterSetUtils.standardizeApostrophes;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.utilities.FilingNormalization.getSortHeading;
import static edu.cornell.library.integration.indexer.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;
import edu.cornell.library.integration.indexer.MarcRecord.Subfield;
import edu.cornell.library.integration.indexer.utilities.Relator;

/**
 * process the whole 7xx range into a wide variety of fields
 * 
 */
public class TitleChangeResultSetToFields implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
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

		// For each field and/of field group, add to SolrInputFields in precedence (field id) order,
		// but with organization determined by vernMode.
		Integer[] ids = sortedFields.keySet().toArray( new Integer[ sortedFields.keySet().size() ]);
		Arrays.sort( ids );
		for( Integer id: ids) {
			FieldSet fs = sortedFields.get(id);
			DataField[] dataFields = fs.fields.toArray( new DataField[ fs.fields.size() ]);
			Collection<CtsField> cts_fields = new ArrayList<CtsField>();
			Collection<String> valuesPersAFacet = new ArrayList<String>();
			Collection<String> valuesCorpAFacet = new ArrayList<String>();
			Collection<String> valuesEventAFacet = new ArrayList<String>();
			Collection<String> valuesATFacet = new ArrayList<String>();
			String relation = null;
			for (DataField f: dataFields) {
				String title_cts = f.concatenateSpecificSubfields("t");
				String author_cts;
				if (f.mainTag.equals("700") || 
						f.mainTag.equals("710") ||
						f.mainTag.equals("711")) {

					title_cts = f.concatenateSpecificSubfields("tklfnpmors");
					if (f.mainTag.equals("700")) {
						author_cts = f.concatenateSpecificSubfields("abcdq");
					} else {
						author_cts = f.concatenateSpecificSubfields("ab");
					}

					// no title - this is an added entry author/creator
					if (title_cts.isEmpty()) {
						relation = "author_addl";
						String author_disp = f.concatenateSpecificSubfields("abcfghijklmnopqrstuvwxyz");
						String author_suffixes = f.concatenateSpecificSubfields("d");
						if ( ! author_suffixes.isEmpty() )
							author_suffixes = " "+author_suffixes;
						for (Subfield sf : f.subfields.values()) {
							if (sf.code.equals('4')) {
								String code = sf.value.toLowerCase().replaceAll("[^a-z]", "");
								try {
									author_suffixes += ", "+Relator.valueOf(code).toString();
								} catch (IllegalArgumentException e) {
									System.out.println("Unexpected relator code: \""+sf.value+"\".");
								}
							}
							else if (sf.code.equals('e'))
								author_suffixes += ", "+sf.value;
						}
						if (author_suffixes.isEmpty() || f.tag.equals("880"))
							cts_fields.add(new CtsField(f.tag.equals("880")?true:false,
									"author_addl",author_disp,author_cts));
						else
							cts_fields.add(new CtsField(false,
									"author_addl",author_disp+author_suffixes,author_cts));
						if (f.mainTag.equals("700"))
							valuesPersAFacet.add(f.concatenateSpecificSubfields("abcdq"));
						else if (f.mainTag.equals("710"))
							valuesCorpAFacet.add(f.concatenateSpecificSubfields("abcdfghijklmnopqrstuvwxyz"));
						else
							valuesEventAFacet.add(f.concatenateSpecificSubfields("abcdfghijklmnopqrstuvwxyz"));
					// with title, this is an included or related work
					} else {
						if (f.ind2.equals('2')) {
							relation = "included_work";
							valuesATFacet.add(f.concatenateSubfieldsOtherThan("6"));
						} else {
							relation = "related_work";
						}
						String workField = f.concatenateSpecificSubfields("iabchqdeklxftgjmnoprsuvwyz");
						cts_fields.add(new CtsField(f.tag.equals("880")?true:false,true,
								relation+"_display",workField,title_cts,author_cts));
					}
					continue; //next datafield
				}

				// included or related work without author metadata
				if (f.mainTag.equals("730") || f.mainTag.equals("740")) {
					
					String workField;
					if (f.mainTag.equals("730")) {
						title_cts = f.concatenateSubfieldsOtherThan("6");
						workField = f.concatenateSpecificSubfields("iaplskfmnordgh");
					} else {
						title_cts = f.concatenateSpecificSubfields("ab");
						workField = f.concatenateSpecificSubfields("iabchqdeklxftgjmnoprsuvwyz");
					}
					if (f.ind2.equals('2'))
						relation = "included_work";
					else 
						relation = "related_work";
					cts_fields.add(new CtsField(f.tag.equals("880")?true:false,
							relation+"_display",workField,title_cts));
					continue; //next datafield
				}

				MAIN: switch (f.mainTag) {
				case "780":
					switch (f.ind2) {
					case '0':
						relation = "continues";			break MAIN;
					case '1':
						relation = "continues_in_part";	break MAIN;
					case '2':
					case '3':
						relation = "supersedes";		break MAIN;
					case '4':
						relation = "merger_of";			break MAIN;
					case '5':
						relation = "absorbed";			break MAIN;
					case '6':
						relation = "absorbed_in_part";	break MAIN;
					case '7':
						relation = "separated_from";	break MAIN;
					}
					break MAIN;
				case "785":
					switch (f.ind2) {
					case '0':
						relation = "continued_by";			break MAIN;
					case '1':
						relation = "continued_in_part_by";	break MAIN;
					case '2':
					case '3':
						relation = "superseded_by";			break MAIN;
					case '4':
						relation = "absorbed_by";			break MAIN;
					case '5':
						relation = "absorbed_in_part_by";	break MAIN;
					case '6':
						relation = "split_into";			break MAIN;
					case '7':
						//Should never display from 785
						relation = "merger";				break MAIN;
					}
					break MAIN;
				case "765":
					relation = "translation_of";	break MAIN;
				case "767":
					relation = "has_translation";	break MAIN;
				case "775":
					relation = "other_edition";		break MAIN;
				case "770":
					relation = "has_supplement";	break MAIN;
				case "772":
					relation = "supplement_to";		break MAIN;
				case "776":
					relation = "other_form";		break MAIN;
				case "777":
					relation = "issued_with";		break MAIN;
				}
				if (relation != null) {
					// display no longer dependant on ind1 = '0'.
					String displaystring = f.concatenateSpecificSubfields("iatbcdgkqrsw");
					cts_fields.add(new CtsField(f.tag.equals("880")?true:false,
							relation+"_display",displaystring,title_cts));
				}

				if (f.mainTag.equals("780") 
						|| f.mainTag.equals("785")
						|| f.mainTag.equals("765")
						|| f.mainTag.equals("767")
						|| f.mainTag.equals("775")
						|| f.mainTag.equals("770")
						|| f.mainTag.equals("772")
						|| f.mainTag.equals("776")
						|| f.mainTag.equals("777")
						) {
					String subfields = "atbcdegkqrs";
					String value = f.concatenateSpecificSubfields(subfields); 
					addField(solrFields,"title_uniform_t",standardizeApostrophes(value));
					if (f.tag.equals("880")) {
						if (f.getScript().equals(MarcRecord.Script.CJK)) {
							addField(solrFields,"title_uniform_t_cjk",value);
						} else {
							if (hasCJK(value))
								addField(solrFields,"title_uniform_t_cjk",value);
						}
					} else {
						if (isCJK(value))
							addField(solrFields,"title_uniform_t_cjk",value);
					}
				}


			}
			// Iff these are an cleanly matched pair of author display fields, do combined encoding
			if (cts_fields.size() == 2) {
				boolean candidate = true;
				CtsField vernField = null;
				CtsField romanField = null;
				for (CtsField f: cts_fields) {
					if (! f.relation.equals("author_addl"))
						candidate = false;
					if (f.vern)
						vernField = f;
					else romanField = f;
				}
				if (candidate && vernField != null && romanField != null) {
					addField(solrFields,"author_addl_display",vernField.display+" / "+romanField.display);
					addField(solrFields,"author_addl_cts",String.format("%s|%s|%s|%s",
							vernField.display,vernField.cts1,romanField.display,romanField.cts1));
					cts_fields.clear();
				}
			}
			for (CtsField f : cts_fields)
				if (f.vern)
					addCtsField(solrFields,f);
			for (CtsField f : cts_fields)
				if ( ! f.vern)
					addCtsField(solrFields,f);
			for (String s : valuesPersAFacet) {
				addField(solrFields,"author_pers_filing",getSortHeading(s));
				addField(solrFields,"author_facet",removeTrailingPunctuation(s,",. "));
			}
			for (String s : valuesCorpAFacet) {
				addField(solrFields,"author_corp_filing",getSortHeading(s));
				addField(solrFields,"author_facet",removeTrailingPunctuation(s,",. "));
			}
			for (String s : valuesEventAFacet) {
				addField(solrFields,"author_event_filing",getSortHeading(s));
				addField(solrFields,"author_facet",removeTrailingPunctuation(s,",. "));
			}
			for (String s : valuesATFacet) {
				addField(solrFields,"authortitle_filing",getSortHeading(s));
				addField(solrFields,"authortitle_facet",removeTrailingPunctuation(s,",. "));
			}
		}
		return solrFields;	
	}
	
	public void addCtsField(Map<String,SolrInputField> solrFields, CtsField f) {
		if (f.relation.equals("author_addl")) {
			addField(solrFields,"author_addl_display",f.display);
			addField(solrFields,"author_addl_cts",String.format("%s|%s",
					f.display,f.cts1));
		} else {
			if (f.cts2 == null)
				addField(solrFields,f.relation,String.format("%s|%s",f.display,f.cts1));
			else 
				addField(solrFields,f.relation,String.format("%s|%s|%s",f.display,f.cts1,f.cts2));
		}
	}

	public class CtsField {
		public String relation;
		public String display;
		public String cts1;
		public String cts2 = null;
		public boolean vern;

		public CtsField (boolean vernacular, String rel, String f, String click1) {
			vern = vernacular;
			relation = rel;
			display = f;
			cts1 = click1;
		}
		public CtsField (boolean vernacular, boolean authortitle, String rel,
				String f, String click1, String click2) {
			vern = vernacular;
			relation = rel;
			display = f;
			cts1 = click1;
			cts2 = click2;
		}
	}
}
