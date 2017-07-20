package edu.cornell.library.integration.indexer;

import java.util.Arrays;
import java.util.List;

import edu.cornell.library.integration.indexer.documentPostProcess.*;
import edu.cornell.library.integration.indexer.fieldMaker.FieldMaker;
import edu.cornell.library.integration.indexer.fieldMaker.SPARQLFieldMakerImpl;
import edu.cornell.library.integration.indexer.solrFieldGen.*;

public class RecordToDocumentMARC extends RecordToDocumentBase {

  @Override
  List<? extends DocumentPostProcess> getDocumentPostProcess() {
    return Arrays.asList(
    		new MissingTitleReport(),
    		new ModifyCallNumbers(),
    		new BarcodeSearch(),
    		new RemoveDuplicateTitleData(),
    		new Collections(),
    		new UpdateSolrInventoryDB());
  }

  @Override
  List<? extends FieldMaker> getFieldMakers() {
    return Arrays.asList(

        new SPARQLFieldMakerImpl().setName("holdings_data")
        .addQuery("control_fields",standardHoldingsControlFieldGroupSPARQL("marcrdf:ControlFields"))
        .addQuery("data_fields",standardHoldingsDataFieldGroupSPARQL("marcrdf:DataFields"))
        .addQuery("description", standardDataFieldSPARQL("300"))
        .addQuery("leader",standardLeaderSPARQL())
        .addResultSetToFields(new HoldingsAndItems()),

        new SPARQLFieldMakerImpl().setName("boost")
        .addQuery("001", standardControlFieldSPARQL("001"))
        .addResultSetToFields(new RecordBoost()),

        new SPARQLFieldMakerImpl().setName("marc")
        .addQuery("leader",standardLeaderSPARQL())
        .addQuery("marc_control_fields",
            "SELECT * WHERE {\n"
                + " $recordURI$ ?p ?field.\n"
                + " ?p rdfs:subPropertyOf marcrdf:ControlFields.\n"
                + " ?field marcrdf:tag ?tag.\n"
                + " ?field marcrdf:value ?value. }")
        .addQuery("marc_data_fields", standardDataFieldGroupSPARQL("marcrdf:DataFields"))
        .addResultSetToFields(new MARC()),

        new SPARQLFieldMakerImpl().setName("rec_type")
        .addQuery("948",standardDataFieldSPARQL("948"))
        .addQuery("holdings_852",standardHoldingsDataFieldSPARQL("852"))
        .addResultSetToFields(new RecordType()),

        new SPARQLFieldMakerImpl().setName("format")
        .addQuery("leader",standardLeaderSPARQL())
        .addQuery("007", standardControlFieldSPARQL("007"))
        .addQuery("008", standardControlFieldSPARQL("008"))
        .addQuery("245", standardDataFieldSPARQL("245"))
        .addQuery("502", standardDataFieldSPARQL("502"))
        .addQuery("653", standardDataFieldSPARQL("653"))
        .addQuery("948", standardDataFieldSPARQL("948"))
        .addQuery("holdings_852",standardHoldingsDataFieldSPARQL("852"))
        .addResultSetToFields(new Format()),

        getLanguageFieldMaker(),

        new SPARQLFieldMakerImpl().setName("call_numbers")
        .addQuery("holdings_callno",standardHoldingsDataFieldSPARQL("852"))
        .addQuery("bib_050callno",standardDataFieldSPARQL("050"))
        .addQuery("bib_950callno",standardDataFieldSPARQL("950"))
        .addResultSetToFields(new CallNumber()),

        new SPARQLFieldMakerImpl().setName("pub_info")
        .addQuery("eight", standardControlFieldSPARQL("008"))
        .addQuery("pub_info_260", standardDataFieldSPARQL("260"))
        .addQuery("pub_info_264", standardDataFieldSPARQL("264"))
        .addResultSetToFields(new PubInfo()),

        new SPARQLFieldMakerImpl().setName("title130")
        .addQuery("title_130", standardDataFieldSPARQL("130"))
        .addResultSetToFields(new Title130()),

        new SPARQLFieldMakerImpl().setName("title_changes")
        .addQuery("added_entry", standardDataFieldGroupSPARQL("marcrdf:AddedEntry"))
        .addQuery("linking_entry", standardDataFieldGroupSPARQL("marcrdf:LinkingEntry"))
        .addResultSetToFields(new TitleChange()),

        new SPARQLFieldMakerImpl().setName("title_series_display")
        .addQuery("title_series", standardDataFieldGroupSPARQL("marcrdf:Series"))
        .addResultSetToFields(new TitleSeries()),

        new SPARQLFieldMakerImpl().setName("author and main title")
        .addQuery("title", standardDataFieldSPARQL("245"))
        .addQuery("title_240", standardDataFieldSPARQL("240"))
        .addQuery("main_entry", standardDataFieldGroupSPARQL("marcrdf:MainEntryAuthor"))
        .addResultSetToFields(new AuthorTitle()),

        new SPARQLFieldMakerImpl().setName("table of contents")
        .addQuery("table of contents", standardDataFieldSPARQL("505"))
        .addResultSetToFields(new TOC()),

        new SPARQLFieldMakerImpl().setName("notes")
        .addQuery("notes", standardDataFieldGroupSPARQL("marcrdf:SimpleProcFields"))
        .addResultSetToFields(new SimpleProc()),

        new SPARQLFieldMakerImpl().setName("urls")
        .addQuery("urls", standardDataFieldSPARQL("856"))
        .addQuery("locations", standardHoldingsDataFieldSPARQL("852"))
        .addQuery("urls_mfhd", standardHoldingsDataFieldSPARQL("856"))
        .addResultSetToFields(new URL()),

        getHathiLinks(),

        new SPARQLFieldMakerImpl().setName("database codes")
        .addQuery("record-level instructions",
            "SELECT ?v WHERE {\n"
                + "  $recordURI$ marcrdf:hasField899 ?f.\n"
                + "  ?f marcrdf:hasSubfield ?sf.\n"
                + "  ?sf marcrdf:code \"a\"^^xsd:string.\n"
                + "  ?sf marcrdf:value ?v. }")
        .addResultSetToFields(new DBCode()),

        new SPARQLFieldMakerImpl().setName("citation_reference_note")
        .addQuery("field510", standardDataFieldSPARQL("510"))
        .addResultSetToFields(new CitationReferenceNote()),

        new SPARQLFieldMakerImpl().setName("instrumentation")
        .addQuery("instrumentation", standardDataFieldSPARQL("382"))
        .addResultSetToFields(new Instrumentation()),

        new SPARQLFieldMakerImpl().setName("finding_aids_index_notes")
        .addQuery("finding_index_notes", standardDataFieldSPARQL("555"))
        .addResultSetToFields(new FindingAids()),

        new SPARQLFieldMakerImpl().setName("fact_or_fiction")
        .addQuery("eight", standardControlFieldSPARQL("008"))
        .addQuery("leader",standardLeaderSPARQL())
        .addResultSetToFields(new FactOrFiction()),

        new SPARQLFieldMakerImpl().setName("subject")
        .addQuery("subjects", standardDataFieldGroupSPARQL("marcrdf:SubjectTermEntry"))
        .addResultSetToFields(new Subject()),

        new SPARQLFieldMakerImpl().setName("newbooks")
        .addQuery("newbooks948",
            "SELECT ?ind1 ?a WHERE {"
                + " $recordURI$ marcrdf:hasField948 ?field.\n"
                + " ?field marcrdf:ind1 \"1\"^^xsd:string. \n"
                + " ?field marcrdf:hasSubfield ?sfield .\n"
                + " ?sfield marcrdf:code \"a\"^^xsd:string .\n"
                + " ?sfield marcrdf:value ?a.\n" + " }")
        .addQuery("newbooksMfhd",
            "SELECT ?five ?code ?x ?z WHERE {"
                + " ?mfhd marcrdf:hasBibliographicRecord $recordURI$.\n"
                + " ?mfhd marcrdf:hasField852 ?mfhd852.\n"
                + " ?mfhd852 marcrdf:hasSubfield ?mfhd852b.\n"
                + " ?mfhd852b marcrdf:code \"b\"^^xsd:string.\n"
                + " ?mfhd852b marcrdf:value ?code. \n"
                + " ?mfhd marcrdf:hasField005 ?mfhd005.\n"
                + " ?mfhd005 marcrdf:value ?five. \n"
                + " OPTIONAL {\n"
                + "   ?mfhd852 marcrdf:hasSubfield ?mfhd852x.\n"
                + "   ?mfhd852x marcrdf:code \"x\"^^xsd:string.\n"
                + "   ?mfhd852x marcrdf:value ?x.  }\n"
                + " OPTIONAL {\n"
                + "   ?mfhd852 marcrdf:hasSubfield ?mfhd852z.\n"
                + "   ?mfhd852z marcrdf:code \"z\"^^xsd:string.\n"
                + "   ?mfhd852z marcrdf:value ?z. } }")
        .addQuery("seven",
            "SELECT (SUBSTR(?seven,1,1) as ?cat) WHERE {"
                + " $recordURI$ marcrdf:hasField007 ?f.\n"
                + " ?f marcrdf:value ?seven. }")
        .addQuery("k",
            "SELECT ?callnumprefix WHERE {"
                + " ?mfhd marcrdf:hasBibliographicRecord $recordURI$.\n"
                + " ?mfhd marcrdf:hasField852 ?mfhd852.\n"
                + " ?mfhd852 marcrdf:hasSubfield ?mfhd852k.\n"
                + " ?mfhd852k marcrdf:code \"k\"^^xsd:string.\n"
                + " ?mfhd852k marcrdf:value ?callnumprefix. }")
        .addResultSetToFields(new NewBooks()),

        new SPARQLFieldMakerImpl().setName("isbn")
        .addQuery("isbn", standardDataFieldSPARQL("020"))
        .addResultSetToFields(new ISBN())
    );
  }

  public static SPARQLFieldMakerImpl getHathiLinks() {
    return new SPARQLFieldMakerImpl().setName("hathi links")
        .addQuery("oclcid", standardDataFieldSPARQL("035"))
        .addQuery("903_barcode", standardDataFieldSPARQL("903"))
        .addResultSetToFields(new HathiLinks());
  }

  public static SPARQLFieldMakerImpl getLanguageFieldMaker() {
    return new SPARQLFieldMakerImpl().setName("language")
        .addQuery("language_008", standardControlFieldSPARQL("008"))
        .addQuery("language_note", standardDataFieldSPARQL("546"))
        .addQuery("languages_041", standardDataFieldSPARQL("041"))
        .addResultSetToFields(new Language());
  }

  private static String standardLeaderSPARQL() {
    return "SELECT * WHERE {\n"
        + " $recordURI$ marcrdf:leader ?leader. }";
 }

  private static String standardControlFieldSPARQL(String tag) {
    return "SELECT * WHERE {\n"
        + " $recordURI$ marcrdf:hasField" + tag + " ?field.\n"
        + " ?field marcrdf:tag ?tag. \n"
        + " ?field marcrdf:value ?value. }";
  }

  private static String standardDataFieldSPARQL(String tag) {
    return "SELECT * WHERE {\n"
        + " BIND( \""+tag+"\"^^xsd:string as ?p ) \n"
        + " $recordURI$ marcrdf:hasField" + tag + " ?field.\n"
        + " ?field marcrdf:tag ?tag. \n"
        + " ?field marcrdf:ind1 ?ind1. \n"
        + " ?field marcrdf:ind2 ?ind2. \n"
        + " ?field marcrdf:hasSubfield ?sfield .\n"
        + " ?sfield marcrdf:code ?code.\n"
        + " ?sfield marcrdf:value ?value. }";
  }

  private static String standardHoldingsControlFieldGroupSPARQL(String group) {
    return "SELECT * WHERE {\n"
        + " ?mfhd marcrdf:hasBibliographicRecord $recordURI$.\n"
        + " ?mfhd ?p ?field.\n"
        + " ?p rdfs:subPropertyOf " + group + ".\n"
        + " ?field marcrdf:tag ?tag. \n"
        + " ?field marcrdf:value ?value. }";
  }

  private static String standardHoldingsDataFieldGroupSPARQL(String group) {
    return "SELECT * WHERE {\n"
        + " ?mfhd marcrdf:hasBibliographicRecord $recordURI$.\n"
        + " ?mfhd ?p ?field.\n"
        + " ?p rdfs:subPropertyOf " + group + ".\n"
        + " ?field marcrdf:tag ?tag. \n"
        + " ?field marcrdf:ind1 ?ind1. \n"
        + " ?field marcrdf:ind2 ?ind2. \n"
        + " ?field marcrdf:hasSubfield ?sfield .\n"
        + " ?sfield marcrdf:code ?code.\n"
        + " ?sfield marcrdf:value ?value. }";
  }

  private static String standardHoldingsDataFieldSPARQL(String tag) {
    return "SELECT * WHERE {\n"
        + " BIND( \""+tag+"\"^^xsd:string as ?p ) \n"
        + " ?mfhd marcrdf:hasBibliographicRecord $recordURI$.\n"
        + " ?mfhd marcrdf:hasField" + tag + " ?field.\n"
        + " ?field marcrdf:tag ?tag.\n"
        + " ?field marcrdf:ind1 ?ind1.\n"
        + " ?field marcrdf:ind2 ?ind2.\n"
        + " ?field marcrdf:hasSubfield ?sfield.\n"
        + " ?sfield marcrdf:code ?code.\n"
        + " ?sfield marcrdf:value ?value. }";
  }

  private static String standardDataFieldGroupSPARQL(String group) {
    return "SELECT * WHERE {"
        + " $recordURI$ ?p ?field.\n"
        + " ?p rdfs:subPropertyOf " + group + ".\n"
        + " ?field marcrdf:tag ?tag. \n"
        + " ?field marcrdf:ind1 ?ind1. \n"
        + " ?field marcrdf:ind2 ?ind2. \n"
        + " ?field marcrdf:hasSubfield ?sfield .\n"
        + " ?sfield marcrdf:code ?code.\n"
        + " ?sfield marcrdf:value ?value. }";
  }
}
