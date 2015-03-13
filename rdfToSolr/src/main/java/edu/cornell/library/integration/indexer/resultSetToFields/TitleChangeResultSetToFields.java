package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.ilcommons.util.CharacterSetUtils.hasCJK;
import static edu.cornell.library.integration.ilcommons.util.CharacterSetUtils.isCJK;
import static edu.cornell.library.integration.ilcommons.util.CharacterSetUtils.standardizeApostrophes;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.getSortHeading;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.removeTrailingPunctuation;

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

/**
 * process the whole 7xx range into a wide variety of fields
 * 
 */
public class TitleChangeResultSetToFields implements ResultSetToFields {

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

		// For each field and/of field group, add to SolrInputFields in precedence (field id) order,
		// but with organization determined by vernMode.
		Integer[] ids = sortedFields.keySet().toArray( new Integer[ sortedFields.keySet().size() ]);
		Arrays.sort( ids );
		for( Integer id: ids) {
			FieldSet fs = sortedFields.get(id);
			DataField[] dataFields = fs.fields.toArray( new DataField[ fs.fields.size() ]);
			Set<String> values880 = new HashSet<String>();
			Set<String> valuesMain = new HashSet<String>();
			String relation = null;
			String mainTag = null;
			for (DataField f: dataFields) {
				mainTag = f.mainTag;
				String title_cts = f.concatenateSpecificSubfields("t");
				String author_cts;
				if (f.mainTag.equals("700")) {
					author_cts = f.concatenateSpecificSubfields("abcdq");
				} else {
					author_cts = f.concatenateSpecificSubfields("ab");
				}
				if (f.mainTag.equals("700") || f.mainTag.equals("710") 
						|| f.mainTag.equals("711")) {
					title_cts = f.concatenateSpecificSubfields("tklfnpmors");
					if (title_cts.length() < 2) {
						relation = "author_addl";
					} else if (f.ind2.equals('2')) {
						if (relation == null)
							relation = "included_work";
					} else {
						if (relation == null)
							relation = "related_work";
					}
				} else if (f.mainTag.equals("730") || f.mainTag.equals("740")) {
					
					if (f.mainTag.equals("730")) {
						title_cts = f.concateSubfieldsOtherThan6();
					} else {
						title_cts = author_cts;
					}
					author_cts = "";
					if (f.ind2.equals('2'))
						relation = "included_work";
					else 
						relation = "related_work";
				}
				if ((relation != null) && relation.equals("author_addl")) {
					if (f.tag.equals("880")) {
						String author_disp = f.concatenateSpecificSubfields("abcefghijklmnopqrstuvwxyz");
						values880.add("author_addl_ctsZ"+author_disp + "|" + author_cts);
					} else {
						String author_disp = f.concatenateSpecificSubfields("abcdefghijklmnopqrstuvwxyz");
						valuesMain.add("author_addl_ctsZ"+author_disp + "|" + author_cts);
					}
				} else if (relation != null) {
					String workField;
					if (f.mainTag.equals("730"))
						workField = f.concatenateSpecificSubfields("iaplskfmnordgh");
					else
						workField = f.concatenateSpecificSubfields("iabchqdeklxftgjmnoprsuvwyz");
					workField += "|"+title_cts;
					if (author_cts.length() > 0)
						workField += "|"+author_cts;
					if (f.tag.equals("880"))
						values880.add(relation+"_displayZ"+workField);
					else 
						valuesMain.add(relation+"_displayZ"+workField);
				}
				relation = "";
				if (title_cts.equals("")) {
					continue;				
				}
				if (f.mainTag.equals("780")) {
					if (f.ind2.equals('0')) {
						relation = "continues";
					} else if (f.ind2.equals('1')) {
						relation = "continues_in_part";
					} else if (f.ind2.equals('2') || f.ind2.equals('3')) {
						relation = "supersedes";
					} else if (f.ind2.equals('4')){
						relation = "merger_of";
					} else if (f.ind2.equals('5')) {
						relation = "absorbed";
					} else if (f.ind2.equals('6')) {
						relation = "absorbed_in_part";
					} else if (f.ind2.equals('7')) {
						relation = "separated_from";
					}
				} else if (f.mainTag.equals("785")) {
					if (f.ind2.equals('0')) {
						relation = "continued_by";
					} else if (f.ind2.equals('1')) {
						relation = "continued_in_part_by";
					} else if (f.ind2.equals('2') || f.ind2.equals('3')) {
						relation = "superseded_by";
					} else if (f.ind2.equals('4'))  {
						relation = "absorbed_by";
					} else if (f.ind2.equals('5')) {
						relation = "absorbed_in_part_by";
					} else if (f.ind2.equals('6')) {
						relation = "split_into";
					} else if (f.ind2.equals('7')) {
						relation = "merger"; //Should never display from 785
					}
				} else if (f.mainTag.equals("765")) {
					relation = "translation_of";
				} else if (f.mainTag.equals("767")) {
					relation = "has_translation";
				} else if (f.mainTag.equals("775")) {
					relation = "other_edition";
				} else if (f.mainTag.equals("770")) {
					relation = "has_supplement";
				} else if (f.mainTag.equals("772")) {
					relation = "supplement_to";
				} else if (f.mainTag.equals("776")) {
					relation = "other_form";
				} else if (f.mainTag.equals("777")) {
					relation = "issued_with";
				}
				if (! relation.equals("")) {
					if (f.ind1.equals('0')) {
						String displaystring = f.concatenateSpecificSubfields("iatbcdgkqrsw")+'|'+ title_cts;
						if (f.tag.equals("880"))
							values880.add(relation+"_displayZ"+displaystring);
						else
							valuesMain.add(relation+"_displayZ"+displaystring);
					}
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
			if ((values880.size() == 1) && (valuesMain.size() == 1)) {
				for (String s:values880) {
					if (s.startsWith("author_")) {
						StringBuilder sb = new StringBuilder();
						String[] temp = s.split("Z",2);
						String[] temp2 = temp[1].split("\\|",2);
						String vernName = temp2[0];
						String name = null;
						sb.append(removeTrailingPunctuation(temp[1],","));
						for (String t:valuesMain) {
							String[] temp3 = t.split("Z",2);
							String[] temp4 = temp3[1].split("\\|",2);
							name = temp4[0];
							sb.append("|");
							sb.append(temp3[1]);
						}
						addField(solrFields,"author_addl_cts",sb.toString());
						addField(solrFields,"author_addl_display",vernName+" / "+name);
						addField(solrFields,"author_"+mainTag+"_exact",getSortHeading(vernName+" / "+name));
						values880.clear();
						valuesMain.clear();
					}
				}
			}
			for (String s: values880) {
				String[] temp = s.split("Z",2);
				addField(solrFields,temp[0],temp[1]);
				if (temp[0].startsWith("author_")) {
					String[] temp2 = temp[1].split("\\|",2);
					addField(solrFields,"author_addl_display",temp2[0]);
					addField(solrFields,"author_"+mainTag+"_exact",getSortHeading(temp2[0]));
				}
			}
			for (String s: valuesMain) {
				String[] temp = s.split("Z",2);
				addField(solrFields,temp[0],temp[1]);
				if (temp[0].startsWith("author_")) {
					String[] temp2 = temp[1].split("\\|",2);
					addField(solrFields,"author_addl_display",temp2[0]);
					addField(solrFields,"author_"+mainTag+"_exact",getSortHeading(temp2[0]));
				}
			}
		}
		return solrFields;	
	}	
}
