package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.hasCJK;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.isCJK;
import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;
import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;
import edu.cornell.library.integration.indexer.MarcRecord.FieldValues;
import edu.cornell.library.integration.indexer.utilities.AuthorityData;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadType;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadTypeDesc;
import edu.cornell.library.integration.indexer.utilities.RelatorSet;

/**
 * process the whole 7xx range into a wide variety of fields
 *
 */
public class TitleChange implements ResultSetToFields {

	static ObjectMapper mapper = new ObjectMapper();

	@Override
	public Map<String, SolrInputField> toFields(
			final Map<String, ResultSet> results, final SolrBuildConfig config) throws Exception {

		Collection<FieldSet> sets = ResultSetUtilities.resultSetsToSetsofMarcFields(results);

		Map<String,SolrInputField> solrFields = new HashMap<>();
		for( FieldSet fs: sets ) {

			final Collection<CtsField> cts_fields = new ArrayList<>();
			final Collection<String> valuesPersAFacet = new ArrayList<>();
			final Collection<String> valuesCorpAFacet = new ArrayList<>();
			final Collection<String> valuesEventAFacet = new ArrayList<>();
			final Collection<String> valuesATFacet = new ArrayList<>();
			HeadTypeDesc htd = null;
			String relation = null;
			for (final DataField f: fs.getFields()) {

				// added entry names with or without title metadata
				if (f.mainTag.equals("700") ||
						f.mainTag.equals("710") ||
						f.mainTag.equals("711")) {
					FieldValues ctsVals;
					if (f.mainTag.equals("700")) {
						ctsVals = f.getFieldValuesForNameMaybeTitleField("abcdq;tklnpmors");
						htd = HeadTypeDesc.PERSNAME;
					} else if (f.mainTag.equals("710")) {
						ctsVals = f.getFieldValuesForNameMaybeTitleField("abd;tklnpmors");
						htd = HeadTypeDesc.CORPNAME;
					} else {
						ctsVals = f.getFieldValuesForNameMaybeTitleField("abde;tklpmors");
						htd = HeadTypeDesc.EVENT;
					}

					// no title - this is an added entry author/creator
					if (ctsVals.type.equals(HeadType.AUTHOR)) {
						relation = "author_addl";
						String author_disp = f.concatenateSpecificSubfields("abcfghijklmnopqrstuvwxyz");
						String author_suffixes = f.concatenateSpecificSubfields("d");
						if ( ! author_suffixes.isEmpty() )
							author_suffixes = " "+author_suffixes;
						final RelatorSet rs = new RelatorSet(f);
						if ( ! rs.isEmpty() ) {
							if (author_suffixes.isEmpty()) {
								author_disp =
										RelatorSet.validateForConcatWRelators(author_disp);
							} else {
								author_suffixes =
										RelatorSet.validateForConcatWRelators(author_suffixes);
							}
							author_suffixes += ' '+rs.toString();
						}
						if (author_suffixes.isEmpty())
							if (author_disp.charAt(author_disp.length()-1) == ',')
								author_disp = author_disp.substring(0, author_disp.length()-1);
						if (author_suffixes.isEmpty() || f.tag.equals("880"))
							cts_fields.add(new CtsField(f.tag.equals("880")?true:false,
									"author_addl",author_disp,ctsVals.author));
						else
							cts_fields.add(new CtsField(false,
									"author_addl",author_disp+author_suffixes,ctsVals.author));
						if (f.mainTag.equals("700"))
							valuesPersAFacet.add(f.concatenateSpecificSubfields("abcdq"));
						else if (f.mainTag.equals("710"))
							valuesCorpAFacet.add(f.concatenateSpecificSubfields("ab"));
						else
							valuesEventAFacet.add(f.concatenateSpecificSubfields("abe"));
						// with title, this is an included or related work
					} else {
						htd = HeadTypeDesc.WORK;
						if (f.ind2.equals('2')) {
							relation = "included_work";
							valuesATFacet.add(ctsVals.author+" | "+f.getStringWithoutInitialArticle(ctsVals.title));
						} else {
							relation = "related_work";
						}
						final String workField = f.concatenateSpecificSubfields("iabchqdeklxftgjmnoprsuvwyz");
						cts_fields.add(new CtsField(f.tag.equals("880")?true:false,
								relation+"_display",workField,ctsVals.title,ctsVals.author));
						addField(solrFields,"title_uniform_t",ctsVals.title);
					}					continue; //next datafield
				}
				String title_cts = null;

				// included or related work without author metadata
				if (f.mainTag.equals("730") || f.mainTag.equals("740")) {

					String workField;
					if (f.mainTag.equals("730")) {
						title_cts = f.concatenateSubfieldsOtherThan("6");
						workField = f.concatenateSpecificSubfields("iaplskfmnordgh");
						final String searchField = f.concatenateSpecificSubfields("abcdefghjklmnopqrstuvwxyz");
						addField(solrFields,"title_uniform_t",searchField);
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
					title_cts = f.concatenateSpecificSubfields("s");
					if (title_cts.isEmpty())
						title_cts = f.concatenateSpecificSubfields("t");
					final String subfields = "atbcegkqrs";
					final String value = f.concatenateSpecificSubfields(subfields);
					final String valueWOarticle = f.getStringWithoutInitialArticle(value);
					addField(solrFields,"title_uniform_t",value);
					addField(solrFields,"title_uniform_t",valueWOarticle);
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

				if (relation != null) {
					// display no longer dependant on ind1 = '0'.
					final String displaystring = f.concatenateSpecificSubfields("iatbcdgkqrsw");
					cts_fields.add(new CtsField(f.tag.equals("880")?true:false,
							relation+"_display",displaystring,title_cts));
				}

			}
			// Iff these are an cleanly matched pair of author display fields, do combined encoding
			if (cts_fields.size() == 2) {
				boolean candidate = true;
				CtsField vernField = null;
				CtsField romanField = null;
				for (final CtsField f: cts_fields) {
					if (! f.relation.equals("author_addl"))
						candidate = false;
					if (f.vern)
						vernField = f;
					else romanField = f;
				}
				if (candidate && vernField != null && romanField != null) {

					// clean comma from end of vernacular display value
					if (vernField.display.charAt(vernField.display.length()-1) == ',')
						vernField.display = vernField.display.substring(0, vernField.display.length()-1);

					addField(solrFields,"author_addl_display",vernField.display+" / "+romanField.display);
					addField(solrFields,"author_addl_cts",String.format("%s|%s|%s|%s",
							vernField.display,vernField.cts1,romanField.display,romanField.cts1));
					final Map<String,Object> json = new HashMap<>();
					json.put("name1", vernField.display);
					json.put("search1", vernField.cts1);
					json.put("name2", romanField.display);
					json.put("search2", romanField.cts1);
					json.put("type", htd.toString());
					final AuthorityData authData = new AuthorityData(config,romanField.cts1,htd);
					json.put("authorizedForm", authData.authorized);
					if (authData.authorized && authData.alternateForms != null)
						for (final String altForm : authData.alternateForms) {
							addField(solrFields,"authority_author_t",altForm);
							if (hasCJK(altForm))
								addField(solrFields,"authority_author_t_cjk",altForm);
						}
					final ByteArrayOutputStream jsonstream = new ByteArrayOutputStream();
					mapper.writeValue(jsonstream, json);
					addField(solrFields,"author_addl_json",jsonstream.toString("UTF-8"));
					cts_fields.clear();
				}
			}
			for (final CtsField f : cts_fields)
				if (f.vern)
					addCtsField(config,solrFields,f, htd);
			for (final CtsField f : cts_fields)
				if ( ! f.vern)
					addCtsField(config,solrFields,f, htd);
			for (final String s : valuesPersAFacet) {
				addField(solrFields,"author_pers_filing",getFilingForm(s));
				addField(solrFields,"author_facet",removeTrailingPunctuation(s,",. "));
			}
			for (final String s : valuesCorpAFacet) {
				addField(solrFields,"author_corp_filing",getFilingForm(s));
				addField(solrFields,"author_facet",removeTrailingPunctuation(s,",. "));
			}
			for (final String s : valuesEventAFacet) {
				addField(solrFields,"author_event_filing",getFilingForm(s));
				addField(solrFields,"author_facet",removeTrailingPunctuation(s,",. "));
			}
			for (final String s : valuesATFacet) {
				addField(solrFields,"authortitle_filing",getFilingForm(s));
				addField(solrFields,"authortitle_facet",removeTrailingPunctuation(s,",. "));
			}
		}
		return solrFields;
	}

	public static void addCtsField(final SolrBuildConfig config, final Map<String,SolrInputField> solrFields, final CtsField f, final HeadTypeDesc htd) throws Exception {
		if (f.relation.equals("author_addl")) {
			addField(solrFields,"author_addl_display",f.display);
			addField(solrFields,"author_addl_cts",String.format("%s|%s",
					f.display,f.cts1));
			final Map<String,Object> json = new HashMap<>();
			json.put("name1", f.display);
			json.put("search1", f.cts1);
			json.put("type", htd.toString());
			final AuthorityData authData = new AuthorityData(config,f.cts1,htd);
			json.put("authorizedForm", authData.authorized);
			if (authData.authorized && authData.alternateForms != null)
				for (final String altForm : authData.alternateForms) {
					addField(solrFields,"authority_author_t",altForm);
					if (hasCJK(altForm))
						addField(solrFields,"authority_author_t_cjk",altForm);
				}
			final ByteArrayOutputStream jsonstream = new ByteArrayOutputStream();
			mapper.writeValue(jsonstream, json);
			addField(solrFields,"author_addl_json",jsonstream.toString("UTF-8"));
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

		public CtsField (final boolean vernacular, final String rel, final String f, final String click1) {
			vern = vernacular;
			relation = rel;
			display = f;
			cts1 = click1;
		}
		public CtsField (final boolean vernacular, final String rel,
				final String f, final String click1, final String click2) {
			vern = vernacular;
			relation = rel;
			display = f;
			cts1 = click1;
			cts2 = click2;
		}
	}
}
