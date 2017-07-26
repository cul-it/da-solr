package edu.cornell.library.integration.indexer.solrFieldGen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.JenaResultsToMarcRecord;
import edu.cornell.library.integration.indexer.utilities.SolrFields;
import edu.cornell.library.integration.indexer.utilities.SolrFields.SolrField;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;

/**
 * Process language data into language_display and language_facet
 * Language codes appearing in records are mapped to language names according to the
 * mapping here: http://www.loc.gov/marc/languages/language_code.html
 * No accounting is made here for codes which are deprecated for use in new records,
 * as their meaning, if found, is not changed by their deprecation.
 */
public class Language implements ResultSetToFields, SolrFieldGenerator {

	private static Map<String,Code> codes = new HashMap<>();
	static {
		Arrays.stream(Code.values()).forEach( c -> codes.put(c.toString().toLowerCase(),c) );
	}

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		JenaResultsToMarcRecord.addControlFieldResultSet(rec,results.get("language_008"));
		JenaResultsToMarcRecord.addDataFieldResultSet(rec,results.get("language_note"));
		JenaResultsToMarcRecord.addDataFieldResultSet(rec,results.get("languages_041"));

		Map<String,SolrInputField> fields = new HashMap<>();

		SolrFields vals = generateSolrFields( rec , null );
		for ( SolrField f : vals.fields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);		

		return fields;
	}

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("008","041","546"); }

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, SolrBuildConfig config ) {

		List<String> display = new ArrayList<>();
		List<String> facet = new ArrayList<>();
		List<String> notes = new ArrayList<>();
		Set<String> articles = new LinkedHashSet<>();

		// Suppress "Undetermined"(UND) and "No Linguistic Content"(ZXX)
		// from facet and display (DISCOVERYACCESS-822)

		for (ControlField cf : rec.controlFields) {
			if (! cf.tag.equals("008"))
				continue;
			String langCode = cf.value.substring(35,38).toLowerCase();
			if (langCode.trim().isEmpty() || langCode.equals("|||"))
				continue;
			if ( ! codes.containsKey(langCode)) {
				System.out.println("Language code "+langCode+" not recognized.");
				continue;
			}
			Code c = codes.get(langCode);
			if (c.equals(Code.UND) || c.equals(Code.ZXX))
				continue;
			display.add( c.getLanguageName() );
			facet.add( c.getLanguageName() );
			if (c.getArticles() != null)
				articles.add(c.getArticles());
		}

		for ( DataField f : rec.matchSortAndFlattenDataFields() ) {
			if (f.mainTag.equals("041")) {
				for (Subfield sf : f.subfields) {
					String langCode = sf.value.toLowerCase();
					if (! codes.containsKey(langCode))
						continue;
					Code c = codes.get(langCode);
					if ( c.getArticles() != null )
						articles.add(c.getArticles());
					if ( c.equals(Code.UND) || c.equals(Code.ZXX)
							|| display.contains(c.getLanguageName()))
						continue;
					switch (sf.code) {
					// subfields for faceting and display
					case 'a': case 'd': case 'e': case 'g': case 'j':
						facet.add(c.getLanguageName());
					// subfields for display only
					case 'b': case 'f':
						display.add(c.getLanguageName());
					}
				}
			}

			// language note
			else if (f.mainTag.equals("546")) {
				String value = f.concatenateSpecificSubfields("3ab");
				if (value.charAt(value.length()-1) != '.')
					value += '.';
				notes.add(value);
				Collection<String> matches = new HashSet<>();
				for (String language : display) {
					if (value.contains(language))
						matches.add(language);
				}
				display.removeAll(matches);
			}
		}
		SolrFields vals = new SolrFields();
		Iterator<String> i = facet.iterator();
		while (i.hasNext())
			vals.add(new SolrField("language_facet",i.next()));
		if ( ! display.isEmpty()) {
			List<String> tmp = new ArrayList<>();
			tmp.add(String.join(", ",display)+'.');
			display = tmp;
		}
		display.addAll(notes);
		if (! display.isEmpty())
			vals.add(new SolrField("language_display",String.join(" ", display)));
		for (String articlesForLang : articles)
			vals.add(new SolrField("language_articles_t",articlesForLang));

		return vals;
	}

	/* Code enums are in upper case, which is more traditional for enumerated values, but less convenient
	 * for matching to values found in the records - which will be in lower case. Lower case enums are not
	 * an option here, as two of the established language codes 'int' and 'new' are Java reserved words and
	 * cannot be used in lower case. (Of the two, 'int' is now deprecated in favor of 'ina', but may still
	 * potentially be found in records. The more commonly used of the two, 'new' is not deprecated and does
	 * appear. */
	private enum Code {
        AAR("Afar"),
        ABK("Abkhaz"),
        ACE("Achinese"),
        ACH("Acoli"),
        ADA("Adangme"),
        ADY("Adygei"),
        AFA("Afroasiatic (Other)"),
        AFH("Afrihili (Artificial language)"),
        AFR("Afrikaans",                             "die n"),
        AIN("Ainu"),
        AJM("Aljamía"),
        AKA("Akan"),
        AKK("Akkadian"),
        ALB("Albanian",                              "disa nje"),
        ALE("Aleut"),
        ALG("Algonquian (Other)"),
        ALT("Altai"),
        AMH("Amharic"),
        ANG("English, Old (ca. 450-1100)"),
        ANP("Angika"),
        APA("Apache languages"),
        ARA("Arabic",                                "al el ال"),
        ARC("Aramaic"),
        ARG("Aragonese"),
        ARM("Armenian"),
        ARN("Mapuche"),
        ARP("Arapaho"),
        ART("Artificial (Other)"),
        ARW("Arawak"),
        ASM("Assamese",                              "eta ekhon ezoni edal ezupa"),
        AST("Bable"),
        ATH("Athapascan (Other)"),
        AUS("Australian languages"),
        AVA("Avaric"),
        AVE("Avestan"),
        AWA("Awadhi"),
        AYM("Aymara"),
        AZE("Azerbaijani"),
        BAD("Banda languages"),
        BAI("Bamileke languages"),
        BAK("Bashkir"),
        BAL("Baluchi"),
        BAM("Bambara"),
        BAN("Balinese"),
        BAQ("Basque"),
        BAS("Basa"),
        BAT("Baltic (Other)"),
        BEJ("Beja"),
        BEL("Belarusian"),
        BEM("Bemba"),
        BEN("Bengali"),
        BER("Berber (Other)",                        "yan yat"),
        BHO("Bhojpuri"),
        BIH("Bihari (Other)"),
        BIK("Bikol"),
        BIN("Edo"),
        BIS("Bislama"),
        BLA("Siksika"),
        BNT("Bantu (Other)"),
        BOS("Bosnian"),
        BRA("Braj"),
        BRE("Breton"),
        BTK("Batak"),
        BUA("Buriat"),
        BUG("Bugis"),
        BUL("Bulgarian"),
        BUR("Burmese"),
        BYN("Bilin"),
        CAD("Caddo"),
        CAI("Central American Indian (Other)"),
        CAM("Khmer"),
        CAR("Carib"),
        CAT("Catalan",                               "el els les Ses Lo los Es Sa del de la de l des un une"),
        CAU("Caucasian (Other)"),
        CEB("Cebuano"),
        CEL("Celtic (Other)"),
        CHA("Chamorro"),
        CHB("Chibcha"),
        CHE("Chechen"),
        CHG("Chagatai"),
        CHI("Chinese"),
        CHK("Chuukese"),
        CHM("Mari"),
        CHN("Chinook jargon"),
        CHO("Choctaw"),
        CHP("Chipewyan"),
        CHR("Cherokee"),
        CHU("Church Slavic"),
        CHV("Chuvash"),
        CHY("Cheyenne"),
        CMC("Chamic languages"),
        COP("Coptic"),
        COR("Cornish"),
        COS("Corsican"),
        CPE("Creoles and Pidgins, English-based (Other)"),
        CPF("Creoles and Pidgins, French-based (Other)"),
        CPP("Creoles and Pidgins, Portuguese-based (Other)"),
        CRE("Cree"),
        CRH("Crimean Tatar"),
        CRP("Creoles and Pidgins (Other)"),
        CSB("Kashubian"),
        CUS("Cushitic (Other)"),
        CZE("Czech"),
        DAK("Dakota"),
        DAN("Danish"),
        DAR("Dargwa"),
        DAY("Dayak"),
        DEL("Delaware"),
        DEN("Slavey"),
        DGR("Dogrib"),
        DIN("Dinka"),
        DIV("Divehi"),
        DOI("Dogri"),
        DRA("Dravidian (Other)"),
        DSB("Lower Sorbian"),
        DUA("Duala"),
        DUM("Dutch, Middle (ca. 1050-1350)"),
        DUT("Dutch",                                 "de het een"),
        DYU("Dyula"),
        DZO("Dzongkha"),
        EFI("Efik"),
        EGY("Egyptian"),
        EKA("Ekajuk"),
        ELX("Elamite"),
        ENG("English",                               "the a an"),
        ENM("English, Middle (1100-1500)"),
        EPO("Esperanto"),
        ESK("Eskimo languages"),
        ESP("Esperanto"),
        EST("Estonian"),
        ETH("Ethiopic"),
        EWE("Ewe"),
        EWO("Ewondo"),
        FAN("Fang"),
        FAO("Faroese"),
        FAR("Faroese"),
        FAT("Fanti"),
        FIJ("Fijian"),
        FIL("Filipino"),
        FIN("Finnish"),
        FIU("Finno-Ugrian (Other)"),
        FON("Fon"),
        FRE("French",                                "le les du de la de l des un une"),
        FRI("Frisian"),
        FRM("French, Middle (ca. 1300-1600)"),
        FRO("French, Old (ca. 842-1300)"),
        FRR("North Frisian"),
        FRS("East Frisian"),
        FRY("Frisian"),
        FUL("Fula"),
        FUR("Friulian"),
        GAA("Gã"),
        GAE("Scottish Gaelix"),
        GAG("Galician"),
        GAL("Oromo"),
        GAY("Gayo"),
        GBA("Gbaya"),
        GEM("Germanic (Other)"),
        GEO("Georgian"),
        GER("German",                                "der die das des dem den ein eine einer eines einem einen"),
        GEZ("Ethiopic"),
        GIL("Gilbertese"),
        GLA("Scottish Gaelic"),
        GLE("Irish"),
        GLG("Galician"),
        GLV("Manx"),
        GMH("German, Middle High (ca. 1050-1500)"),
        GOH("German, Old High (ca. 750-1050)"),
        GON("Gondi"),
        GOR("Gorontalo"),
        GOT("Gothic"),
        GRB("Grebo"),
        GRC("Greek, Ancient (to 1453)"),
        GRE("Greek, Modern (1453-)",               "ο η το οι οι τα ένας μια ένα hē tōn ta to ho hoi henas enas o"),
        GRN("Guarani"),
        GSW("Swiss German"),
        GUA("Guarani"),
        GUJ("Gujarati"),
        GWI("Gwich'in"),
        HAI("Haida"),
        HAT("Haitian French Creole"),
        HAU("Hausa"),
        HAW("Hawaiian",                              "ka ke na he"),
        HEB("Hebrew",                                "ha ה "),
        HER("Herero"),
        HIL("Hiligaynon"),
        HIM("Western Pahari languages"),
        HIN("Hindi"),
        HIT("Hittite"),
        HMN("Hmong"),
        HMO("Hiri Motu"),
        HRV("Croatian"),
        HSB("Upper Sorbian"),
        HUN("Hungarian",                             "a az"),
        HUP("Hupa"),
        IBA("Iban"),
        IBO("Igbo"),
        ICE("Icelandic"),
        IDO("Ido"),
        III("Sichuan Yi"),
        IJO("Ijo"),
        IKU("Inuktitut"),
        ILE("Interlingue"),
        ILO("Iloko"),
        INA("Interlingua (International Auxiliary Language Association)"),
        INC("Indic (Other)"),
        IND("Indonesian"),
        INE("Indo-European (Other)"),
        INH("Ingush"),
        INT("Interlingua (International Auxiliary Language Association)"),
        IPK("Inupiaq"),
        IRA("Iranian (Other)"),
        IRI("Irish",                                 "an na"),
        IRO("Iroquoian (Other)"),
        ITA("Italian",                  "il lo la l' i gli le del dello della dell' dei degli delle un uno una un"),
        JAV("Javanese"),
        JBO("Lojban (Artificial language)"),
        JPN("Japanese"),
        JPR("Judeo-Persian"),
        JRB("Judeo-Arabic"),
        KAA("Kara-Kalpak"),
        KAB("Kabyle"),
        KAC("Kachin"),
        KAL("Kalâtdlisut"),
        KAM("Kamba"),
        KAN("Kannada"),
        KAR("Karen languages"),
        KAS("Kashmiri"),
        KAU("Kanuri"),
        KAW("Kawi"),
        KAZ("Kazakh"),
        KBD("Kabardian"),
        KHA("Khasi"),
        KHI("Khoisan (Other)"),
        KHM("Khmer"),
        KHO("Khotanese"),
        KIK("Kikuyu"),
        KIN("Kinyarwanda"),
        KIR("Kyrgyz"),
        KMB("Kimbundu"),
        KOK("Konkani"),
        KOM("Komi"),
        KON("Kongo"),
        KOR("Korean"),
        KOS("Kosraean"),
        KPE("Kpelle"),
        KRC("Karachay-Balkar"),
        KRL("Karelian"),
        KRO("Kru (Other)"),
        KRU("Kurukh"),
        KUA("Kuanyama"),
        KUM("Kumyk"),
        KUR("Kurdish",                               "hende birre"),
        KUS("Kusaie"),
        KUT("Kootenai"),
        LAD("Ladino"),
        LAH("Lahndā"),
        LAM("Lamba (Zambia and Congo)"),
        LAN("Occitan (post 1500)"),
        LAO("Lao"),
        LAP("Sami"),
        LAT("Latin"),
        LAV("Latvian"),
        LEZ("Lezgian"),
        LIM("Limburgish",                            "den dei d' dat dem der daers daeres daer en eng engem enger"),
        LIN("Lingala"),
        LIT("Lithuanian"),
        LOL("Mongo-Nkundu"),
        LOZ("Lozi"),
        LTZ("Luxembourgish"),
        LUA("Luba-Lulua"),
        LUB("Luba-Katanga"),
        LUG("Ganda"),
        LUI("Luiseño"),
        LUN("Lunda"),
        LUO("Luo (Kenya and Tanzania)"),
        LUS("Lushai"),
        MAC("Macedonian"),
        MAD("Madurese"),
        MAG("Magahi"),
        MAH("Marshallese"),
        MAI("Maithili"),
        MAK("Makasar"),
        MAL("Malayalam"),
        MAN("Mandingo"),
        MAO("Maori"),
        MAP("Austronesian (Other)"),
        MAR("Marathi"),
        MAS("Maasai"),
        MAX("Manx"),
        MAY("Malay"),
        MDF("Moksha"),
        MDR("Mandar"),
        MEN("Mende"),
        MGA("Irish, Middle (ca. 1100-1550)"),
        MIC("Micmac"),
        MIN("Minangkabau"),
        MIS("Miscellaneous languages"),
        MKH("Mon-Khmer (Other)"),
        MLA("Malagasy"),
        MLG("Malagasy"),
        MLT("Maltese"),
        MNC("Manchu"),
        MNI("Manipuri"),
        MNO("Manobo languages"),
        MOH("Mohawk"),
        MOL("Moldavian"),
        MON("Mongolian"),
        MOS("Mooré"),
        MUL("Multiple languages"),
        MUN("Munda (Other)"),
        MUS("Creek"),
        MWL("Mirandese"),
        MWR("Marwari"),
        MYN("Mayan languages"),
        MYV("Erzya"),
        NAH("Nahuatl"),
        NAI("North American Indian (Other)"),
        NAP("Neapolitan Italian"),
        NAU("Nauru"),
        NAV("Navajo"),
        NBL("Ndebele (South Africa)"),
        NDE("Ndebele (Zimbabwe)"),
        NDO("Ndonga"),
        NDS("Low German"),
        NEP("Nepali"),
        NEW("Newari"),
        NIA("Nias"),
        NIC("Niger-Kordofanian (Other)"),
        NIU("Niuean"),
        NNO("Norwegian (Nynorsk)",                   "ein eit ei"),
        NOB("Norwegian (Bokmål)",                    "en et ei"),
        NOG("Nogai"),
        NON("Old Norse"),
        NOR("Norwegian"),
        NQO("N'Ko"),
        NSO("Northern Sotho"),
        NUB("Nubian languages"),
        NWC("Newari, Old"),
        NYA("Nyanja"),
        NYM("Nyamwezi"),
        NYN("Nyankole"),
        NYO("Nyoro"),
        NZI("Nzima"),
        OCI("Occitan (post-1500)"),
        OJI("Ojibwa"),
        ORI("Oriya"),
        ORM("Oromo"),
        OSA("Osage"),
        OSS("Ossetic"),
        OTA("Turkish, Ottoman"),
        OTO("Otomian languages"),
        PAA("Papuan (Other)"),
        PAG("Pangasinan"),
        PAL("Pahlavi"),
        PAM("Pampanga"),
        PAN("Panjabi"),
        PAP("Papiamento"),
        PAU("Palauan"),
        PEO("Old Persian (ca. 600-400 B.C.)"),
        PER("Persian"),
        PHI("Philippine (Other)"),
        PHN("Phoenician"),
        PLI("Pali"),
        POL("Polish"),
        PON("Pohnpeian"),
        POR("Portuguese",                            "o a os as um uma uns umas"),
        PRA("Prakrit languages"),
        PRO("Provençal (to 1500)"),
        PUS("Pushto"),
        QUE("Quechua"),
        RAJ("Rajasthani"),
        RAP("Rapanui"),
        RAR("Rarotongan"),
        ROA("Romance (Other)"),
        ROH("Raeto-Romance"),
        ROM("Romani"),
        RUM("Romanian",                              "un o unui unei niste unor"),
        RUN("Rundi"),
        RUP("Aromanian"),
        RUS("Russian"),
        SAD("Sandawe"),
        SAG("Sango (Ubangi Creole)"),
        SAH("Yakut"),
        SAI("South American Indian (Other)"),
        SAL("Salishan languages"),
        SAM("Samaritan Aramaic"),
        SAN("Sanskrit"),
        SAO("Samoan"),
        SAS("Sasak"),
        SAT("Santali"),
        SCC("Serbian"),
        SCN("Sicilian Italian"),
        SCO("Scots"),
        SCR("Croatian"),
        SEL("Selkup"),
        SEM("Semitic (Other)"),
        SGA("Irish, Old (to 1100)"),
        SGN("Sign languages"),
        SHN("Shan"),
        SHO("Shona"),
        SID("Sidamo"),
        SIN("Sinhalese"),
        SIO("Siouan (Other)"),
        SIT("Sino-Tibetan (Other)"),
        SLA("Slavic (Other)"),
        SLO("Slovak"),
        SLV("Slovenian"),
        SMA("Southern Sami"),
        SME("Northern Sami"),
        SMI("Sami"),
        SMJ("Lule Sami"),
        SMN("Inari Sami"),
        SMO("Samoan"),
        SMS("Skolt Sami"),
        SNA("Shona"),
        SND("Sindhi"),
        SNH("Sinhalese"),
        SNK("Soninke"),
        SOG("Sogdian"),
        SOM("Somali"),
        SON("Songhai"),
        SOT("Sotho"),
        SPA("Spanish",                               "el la lo los las un una unos unas"),
        SRD("Sardinian"),
        SRN("Sranan"),
        SRP("Serbian"),
        SRR("Serer"),
        SSA("Nilo-Saharan (Other)"),
        SSO("Sotho"),
        SSW("Swazi"),
        SUK("Sukuma"),
        SUN("Sundanese"),
        SUS("Susu"),
        SUX("Sumerian"),
        SWA("Swahili"),
        SWE("Swedish"),
        SWZ("Swazi"),
        SYC("Syriac"),
        SYR("Syriac, Modern"),
        TAG("Tagalog"),
        TAH("Tahitian"),
        TAI("Tai (Other)"),
        TAJ("Tajik"),
        TAM("Tamil"),
        TAR("Tatar"),
        TAT("Tatar"),
        TEL("Telugu"),
        TEM("Temne"),
        TER("Terena"),
        TET("Tetum"),
        TGK("Tajik"),
        TGL("Tagalog"),
        THA("Thai"),
        TIB("Tibetan"),
        TIG("Tigré"),
        TIR("Tigrinya"),
        TIV("Tiv"),
        TKL("Tokelauan"),
        TLH("Klingon (Artificial language)"),
        TLI("Tlingit"),
        TMH("Tamashek"),
        TOG("Tonga (Nyasa)"),
        TON("Tongan"),
        TPI("Tok Pisin"),
        TRU("Truk"),
        TSI("Tsimshian"),
        TSN("Tswana"),
        TSO("Tsonga"),
        TSW("Tswana"),
        TUK("Turkmen"),
        TUM("Tumbuka"),
        TUP("Tupi languages"),
        TUR("Turkish"),
        TUT("Altaic (Other)"),
        TVL("Tuvaluan"),
        TWI("Twi"),
        TYV("Tuvinian"),
        UDM("Udmurt"),
        UGA("Ugaritic"),
        UIG("Uighur"),
        UKR("Ukrainian"),
        UMB("Umbundu"),
        UND("Undetermined"),
        URD("Urdu"),
        UZB("Uzbek"),
        VAI("Vai"),
        VEN("Venda"),
        VIE("Vietnamese"),
        VOL("Volapük"),
        VOT("Votic"),
        WAK("Wakashan languages"),
        WAL("Wolayta"),
        WAR("Waray"),
        WAS("Washoe"),
        WEL("Welsh",                                 "y yr"),
        WEN("Sorbian (Other)"),
        WLN("Walloon"),
        WOL("Wolof"),
        XAL("Oirat"),
        XHO("Xhosa"),
        YAO("Yao (Africa)"),
        YAP("Yapese"),
        YID("Yiddish",                               "דער der די di דאָס dos דעם dem  אַ a אַן an"),
        YOR("Yoruba"),
        YPK("Yupik languages"),
        ZAP("Zapotec"),
        ZBL("Blissymbolics"),
        ZEN("Zenaga"),
        ZHA("Zhuang"),
        ZND("Zande languages"),
        ZUL("Zulu"),
        ZUN("Zuni"),
        ZXX("No linguistic content"),
        ZZA("Zaza");

		private String langName;
		private String articles;
		private Code(String langName) {
			this.langName = langName;
			this.articles = null;
		}
		private Code(String langName, String articles) {
			this.langName = langName;
			this.articles = articles;
		}

		public String getLanguageName() { return langName; }
		public String getArticles() { return articles; }

	}
}
