package edu.cornell.library.integration.metadata.generator;

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

import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.metadata.support.SupportReferenceData;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.SolrFields;
import edu.cornell.library.integration.utilities.SolrFields.SolrField;

/**
 * Process language data into language_display and language_facet
 * Language codes appearing in records are mapped to language names according to the
 * mapping here: http://www.loc.gov/marc/languages/language_code.html
 * No accounting is made here for codes which are deprecated for use in new records,
 * as their meaning, if found, is not changed by their deprecation.
 */
public class Language implements SolrFieldGenerator {

	private static Map<String,Code> codes = new HashMap<>();
	static {
		Arrays.stream(Code.values()).forEach( c -> codes.put(c.toString().toLowerCase(),c) );
	}

	@Override
	public String getVersion() { return "1.2"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("008","041","546"); }

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config config ) {

		List<String> display = new ArrayList<>();
		List<String> facet = new ArrayList<>();
		List<String> notes = new ArrayList<>();
		Set<String> articles = new LinkedHashSet<>();
		String ianaCode = null;

		// Suppress "Undetermined"(UND) and "No Linguistic Content"(ZXX)
		// from facet and display (DISCOVERYACCESS-822)

		for (ControlField cf : rec.controlFields) {
			if (! cf.tag.equals("008"))
				continue;
			if ( cf.value.length() < 38 ) {
				System.out.println( "Error: Corrupt 008 field on b"+rec.id );
				continue;
			}
			String langCode = cf.value.substring(35,38).toLowerCase();
			if (langCode.trim().isEmpty() || langCode.equals("|||"))
				continue;
			if ( ! codes.containsKey(langCode))
				continue;
			Code c = codes.get(langCode);
			if (c.equals(Code.UND) || c.equals(Code.ZXX))
				continue;
			display.add( c.getLanguageName() );
			facet.add( c.getLanguageName() );
			ianaCode = c.getIanaCode();
			if (c.getArticles() != null)
				articles.add(c.getArticles());
		}

		for ( DataField f : rec.matchSortAndFlattenDataFields() ) {
			if (f.mainTag.equals("041")) {
				for (Subfield sf : f.subfields) {
					if (sf.value.length() % 3 == 0) { for ( int i = 0; i < sf.value.length() / 3; i++ ) {
						String langCode = sf.value.substring(i*3,i*3+3).toLowerCase();
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
					} }
				}
				if ( display.size() > 2 ) display.remove(Code.MUL.getLanguageName());
			}

			// language note
			else if (f.mainTag.equals("546")) {
				String value = f.concatenateSpecificSubfields("3ab");
				if ( value.isEmpty() ) continue;
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
			vals.add("language_facet",i.next());
		if ( ! display.isEmpty()) {
			List<String> tmp = new ArrayList<>();
			tmp.add(String.join(", ",display)+'.');
			display = tmp;
		}
		display.addAll(notes);
		if (! display.isEmpty())
			vals.add("language_display",String.join(" ", display));
		for (String articlesForLang : articles)
			vals.add("language_articles_t",articlesForLang);
		if (ianaCode != null)
			vals.add("language_iana_data", ianaCode);

		return vals;
	}

	@Override
	public SolrFields generateNonMarcSolrFields(Map<String, Object> instance, Config config) {

		List<String> display = new ArrayList<>();
		List<String> facet = new ArrayList<>();
		List<String> notes = new ArrayList<>();
		Set<String> articles = new LinkedHashSet<>();

		if ( instance.containsKey("languages") ) {
			List<String> langs = (List<String>) instance.get("languages");
			for ( String lang : langs ) {
				if ( lang == null ) continue;
				String langCode = lang.trim();
				if ( langCode.isEmpty() || langCode.equals("|||") ) continue;
				if ( ! codes.containsKey(langCode) ) continue;
				Code c = codes.get(langCode);
				if (c.equals(Code.UND) || c.equals(Code.ZXX)) continue;
				display.add(c.getLanguageName());
				facet.add(c.getLanguageName());
				if ( c.getArticles() != null ) articles.add(c.getArticles());
			}
		}

		if ( instance.containsKey("notes") ) {
			List<Map<String,Object>> noteHashes = (List<Map<String, Object>>) instance.get("notes");
			String languageNoteTypeId = SupportReferenceData.instanceNoteTypes.getUuid("Language note");
			if (languageNoteTypeId == null) {
				System.out.println("ERROR No instance note type in Folio matches 'Language note'.");
				System.exit(1);
			}
			for ( Map<String,Object> note : noteHashes ) {
				if ( note.containsKey("instanceNoteTypeId") && 
						! languageNoteTypeId.equals((String)note.get("instanceNoteTypeId"))) continue;
				if ( note.containsKey("staffOnly") && (boolean)note.get("staffOnly")) continue;
				if ( note.containsKey("note") ) {
					String value = (String) note.get("note");
					if ( value.isEmpty() ) continue;
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
		}
		SolrFields vals = new SolrFields();
		Iterator<String> i = facet.iterator();
		while (i.hasNext()) vals.add(new SolrField("language_facet",i.next()));
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
	 * appear.) */
	public enum Code {
	AAR(new Builder("Afar").setIanaCode("aa")),
	ABK(new Builder("Abkhaz").setIanaCode("ab")), // Abkhazian in IANA
	ACE("Achinese"),
	ACH("Acoli"),
	ADA("Adangme"),
	ADY("Adygei"),
	AFA("Afroasiatic (Other)"),
	AFH("Afrihili (Artificial language)"),
	AFR(new Builder("Afrikaans").setIanaCode("af").setArticles("die n")),
	AIN("Ainu"),
	AJM("Aljamía"),
	AKA(new Builder("Akan").setIanaCode("ak")),
	AKK("Akkadian"),
	ALB(new Builder("Albanian").setIanaCode("sq").setArticles("nje")),
	ALE("Aleut"),
	ALG("Algonquian (Other)"),
	ALT("Altai"),
	AMH(new Builder("Amharic").setIanaCode("am")),
	ANG("English, Old (ca. 450-1100)"),
	ANP("Angika"),
	APA("Apache languages"),
	ARA(new Builder("Arabic").setIanaCode("ar").setArticles("al el ال")),
	ARC("Aramaic"),
	ARG(new Builder("Aragonese").setIanaCode("an")),
	ARM(new Builder("Armenian").setIanaCode("hy")),
	ARN("Mapuche"),
	ARP("Arapaho"),
	ART("Artificial (Other)"),
	ARW("Arawak"),
	ASM(new Builder("Assamese").setIanaCode("as").setArticles("eta ekhon ezoni edal ezupa")),
	AST("Bable"),
	ATH("Athapascan (Other)"),
	AUS("Australian languages"),
	AVA(new Builder("Avaric").setIanaCode("av")),
	AVE(new Builder("Avestan").setIanaCode("ae")),
	AWA("Awadhi"),
	AYM(new Builder("Aymara").setIanaCode("ay")),
	AZE(new Builder("Azerbaijani").setIanaCode("az")),
	BAD("Banda languages"),
	BAI("Bamileke languages"),
	BAK(new Builder("Bashkir").setIanaCode("ba")),
	BAL(new Builder("Baluchi").setArticles("al")),
	BAM(new Builder("Bambara").setIanaCode("bm")),
	BAN("Balinese"),
	BAQ(new Builder("Basque").setIanaCode("eu").setArticles("bat")),
	BAS("Basa"),
	BAT("Baltic (Other)"),
	BEJ("Beja"),
	BEL(new Builder("Belarusian").setIanaCode("be")),
	BEM("Bemba"),
	BEN(new Builder("Bengali").setIanaCode("bn")),
	BER(new Builder("Berber (Other)").setArticles("yan yat")),
	BHO("Bhojpuri"),
	BIH(new Builder("Bihari (Other)").setIanaCode("bih")), // code bh says bih is preferred
	BIK("Bikol"),
	BIN("Edo"),
	BIS(new Builder("Bislama").setIanaCode("bi")),
	BLA("Siksika"),
	BNT("Bantu (Other)"),
	BOS(new Builder("Bosnian").setIanaCode("bs")),
	BRA("Braj"),
	BRE(new Builder("Breton").setIanaCode("br").setArticles("al an ar eul eun eur ul un ur")),
	BTK("Batak"),
	BUA("Buriat"),
	BUG("Bugis"),
	BUL(new Builder("Bulgarian").setIanaCode("bg")),
	BUR(new Builder("Burmese").setIanaCode("my")),
	BYN("Bilin"),
	CAD("Caddo"),
	CAI("Central American Indian (Other)"),
	CAM("Khmer"),
	CAR("Carib"),
	CAT(new Builder("Catalan").setIanaCode("ca").setArticles("el els en l la les un una")),
	CAU("Caucasian (Other)"),
	CEB("Cebuano"),
	CEL("Celtic (Other)"),
	CHA(new Builder("Chamorro").setIanaCode("ch")),
	CHB("Chibcha"),
	CHE(new Builder("Chechen").setIanaCode("ce")),
	CHG("Chagatai"),
	CHI(new Builder("Chinese").setIanaCode("zh")),
	CHK("Chuukese"),
	CHM("Mari"),
	CHN("Chinook jargon"),
	CHO("Choctaw"),
	CHP("Chipewyan"),
	CHR("Cherokee"),
	CHU(new Builder("Church Slavic").setIanaCode("cu")),
	CHV(new Builder("Chuvash").setIanaCode("cv")),
	CHY("Cheyenne"),
	CMC("Chamic languages"),
	COP("Coptic"),
	COR(new Builder("Cornish").setIanaCode("kw")),
	COS(new Builder("Corsican").setIanaCode("co")),
	CPE("Creoles and Pidgins, English-based (Other)"),
	CPF("Creoles and Pidgins, French-based (Other)"),
	CPP("Creoles and Pidgins, Portuguese-based (Other)"),
	CRE(new Builder("Cree").setIanaCode("cr")),
	CRH("Crimean Tatar"),
	CRP("Creoles and Pidgins (Other)"),
	CSB("Kashubian"),
	CUS("Cushitic (Other)"),
	CZE(new Builder("Czech").setIanaCode("cs")),
	DAK("Dakota"),
	DAN(new Builder("Danish").setIanaCode("da").setArticles("de den det en et")),
	DAR("Dargwa"),
	DAY("Dayak"),
	DEL("Delaware"),
	DEN("Slavey"),
	DGR("Dogrib"),
	DIN("Dinka"),
	DIV(new Builder("Divehi").setIanaCode("dv")),
	DOI("Dogri"),
	DRA("Dravidian (Other)"),
	DSB("Lower Sorbian"),
	DUA("Duala"),
	DUM(new Builder("Dutch, Middle (ca. 1050-1350)").setArticles("de een eene het n t")),
	DUT(new Builder("Dutch").setIanaCode("nl").setArticles("de een eene het n t")),
	DYU("Dyula"),
	DZO(new Builder("Dzongkha").setIanaCode("dz")),
	EFI("Efik"),
	EGY("Egyptian"),
	EKA("Ekajuk"),
	ELX("Elamite"),
	ENG(new Builder("English").setIanaCode("en").setArticles("the a an")),
	ENM("English, Middle (1100-1500)"),
	EPO(new Builder("Esperanto").setIanaCode("eo").setArticles("la")),
	ESK("Eskimo languages"),
	ESP(new Builder("Esperanto").setIanaCode("eo").setArticles("la")),
	EST(new Builder("Estonian").setIanaCode("et")),
	ETH("Ethiopic"),
	EWE(new Builder("Ewe").setIanaCode("ee")),
	EWO("Ewondo"),
	FAN("Fang"),
	FAO(new Builder("Faroese").setIanaCode("fo")),
	FAR(new Builder("Faroese").setIanaCode("fo")),
	FAT("Fanti"),
	FIJ(new Builder("Fijian").setIanaCode("fj").setArticles("a e dua na")),
	FIL("Filipino"),
	FIN(new Builder("Finnish").setIanaCode("fi")),
	FIU("Finno-Ugrian (Other)"),
	FON("Fon"),
	FRE(new Builder("French").setIanaCode("fr").setArticles("l la le les un une")),
	FRI(new Builder("Frisian").setIanaCode("fy").setArticles("de e in it n t")),// Western Frisian in IANA
	FRM(new Builder("French, Middle (ca. 1300-1600)").setArticles("l la le les un une")),
	FRO(new Builder("French, Old (ca. 842-1300)").setArticles("l li la le les")),
	FRR("North Frisian"),
	FRS("East Frisian"),
	FRY(new Builder("Frisian").setIanaCode("fy").setArticles("de e in it n t")),
	FUL(new Builder("Fula").setIanaCode("ff")),
	FUR("Friulian"),
	GAA("Gã"),
	GAE(new Builder("Scottish Gaelic").setIanaCode("gd").setArticles("a am an t na h")),
	GAG(new Builder("Galician").setIanaCode("gl").setArticles("a as o unha")),
	GAL("Oromo"),
	GAY("Gayo"),
	GBA("Gbaya"),
	GEM("Germanic (Other)"),
	GEO(new Builder("Georgian").setIanaCode("ka")),
	GER(new Builder("German").setIanaCode("de").setArticles("das dem den der des die ein eine einem einen einer eines")),
	GEZ("Ethiopic"),
	GIL("Gilbertese"),
	GLA(new Builder("Scottish Gaelic").setIanaCode("gd").setArticles("a am an t na h")),
	GLE(new Builder("Irish").setIanaCode("ga")),
	GLG(new Builder("Galician").setIanaCode("gl").setArticles("a as o unha")),
	GLV(new Builder("Manx").setIanaCode("gv")),
	GMH("German, Middle High (ca. 1050-1500)"),
	GOH("German, Old High (ca. 750-1050)"),
	GON("Gondi"),
	GOR("Gorontalo"),
	GOT("Gothic"),
	GRB("Grebo"),
	GRC(new Builder("Greek, Ancient (to 1453)")
			.setArticles("hai αἱ hē ἡ ho ὁ hoi οἱ ta τά tain ταῖν tais ταῖς tas τάς tē τῃ "+
			             "tēn τήν tēs τῆς to τό tō τῳ τώ toin τοῖν tois τοῖς ton τόν tōn τῶν tou τοῦ")),
	GRE(new Builder("Greek, Modern (1453-)").setIanaCode("el")
			.setArticles("e η ena ενα enan εναν enas ενασ enos ενοσ hai αι he η heis εισ hen εν hena"+
			             " henan henas henos ho ο hoi οι mia μια mian μιαν mias μιασ o ο oi ta τα τα "+
			             "te τη tes τησ τησ tis τισ to το ton τον των των tou του tous τουσ")),
	GRN(new Builder("Guarani").setIanaCode("gn")),
	GSW("Swiss German"),
	GUA(new Builder("Guarani").setIanaCode("gn")),
	GUJ(new Builder("Gujarati").setIanaCode("gu")),
	GWI("Gwich'in"),
	HAI("Haida"),
	HAT(new Builder("Haitian French Creole").setIanaCode("ht")), // Haitian or Haitian Creole in IANA
	HAU(new Builder("Hausa").setIanaCode("ha")),
	HAW(new Builder("Hawaiian").setArticles("ka ke na he")),
	HEB(new Builder("Hebrew").setIanaCode("he").setArticles("ha ה he")),// iw also listed for IANA, but preffered is he
	HER(new Builder("Herero").setIanaCode("hz")),
	HIL("Hiligaynon"),
	HIM("Western Pahari languages"),
	HIN(new Builder("Hindi").setIanaCode("hi")),
	HIT("Hittite"),
	HMN("Hmong"),
	HMO(new Builder("Hiri Motu").setIanaCode("ho")),
	HRV(new Builder("Croatian").setIanaCode("hr")),
	HSB("Upper Sorbian"),
	HUN(new Builder("Hungarian").setIanaCode("hu").setArticles("a az")),
	HUP("Hupa"),
	IBA("Iban"),
	IBO(new Builder("Igbo").setIanaCode("ig")),
	ICE(new Builder("Icelandic").setIanaCode("is")
			.setArticles("hin hina hinar hinir hinn hinna hinnar hinni hins hinu hinum hið 'r")),
	IDO(new Builder("Ido").setIanaCode("io")),
	III(new Builder("Sichuan Yi").setIanaCode("ii")),
	IJO("Ijo"),
	IKU(new Builder("Inuktitut").setIanaCode("iu")),
	ILE(new Builder("Interlingue").setIanaCode("ie")),
	ILO("Iloko"),
	INA(new Builder("Interlingua (International Auxiliary Language Association)").setIanaCode("ia")),
	INC("Indic (Other)"),
	IND(new Builder("Indonesian").setIanaCode("id")),// in also listed for IANA, but with preferred listed as id
	INE("Indo-European (Other)"),
	INH("Ingush"),
	INT(new Builder("Interlingua (International Auxiliary Language Association)").setIanaCode("ia")),
	IPK(new Builder("Inupiaq").setIanaCode("ik")),
	IRA("Iranian (Other)"),
	IRI(new Builder("Irish").setArticles("an na")),
	IRO("Iroquoian (Other)"),
	ITA(new Builder("Italian").setIanaCode("it").setArticles("gl' gli i il l' la le li lo un una uno")),
	JAV(new Builder("Javanese").setIanaCode("jv")),// jw also listed for IANA, but with preferred listed as id
	JBO("Lojban (Artificial language)"),
	JPN(new Builder("Japanese").setIanaCode("ja")),
	JPR("Judeo-Persian"),
	JRB("Judeo-Arabic"),
	KAA("Kara-Kalpak"),
	KAB("Kabyle"),
	KAC("Kachin"),
	KAL(new Builder("Kalâtdlisut").setIanaCode("kl")),
	KAM("Kamba"),
	KAN(new Builder("Kannada").setIanaCode("kn")),
	KAR("Karen languages"),
	KAS(new Builder("Kashmiri").setIanaCode("ks")),
	KAU(new Builder("Kanuri").setIanaCode("kr")),
	KAW("Kawi"),
	KAZ(new Builder("Kazakh").setIanaCode("kk")),
	KBD("Kabardian"),
	KHA("Khasi"),
	KHI("Khoisan (Other)"),
	KHM(new Builder("Khmer").setIanaCode("km")),
	KHO("Khotanese"),
	KIK(new Builder("Kikuyu").setIanaCode("ki")),
	KIN(new Builder("Kinyarwanda").setIanaCode("rw")),
	KIR(new Builder("Kyrgyz").setIanaCode("ky")),
	KMB("Kimbundu"),
	KOK("Konkani"),
	KOM(new Builder("Komi").setIanaCode("kv")),
	KON(new Builder("Kongo").setIanaCode("kg")),
	KOR(new Builder("Korean").setIanaCode("ko")),
	KOS("Kosraean"),
	KPE("Kpelle"),
	KRC("Karachay-Balkar"),
	KRL("Karelian"),
	KRO("Kru (Other)"),
	KRU("Kurukh"),
	KUA(new Builder("Kuanyama").setIanaCode("kj")),
	KUM("Kumyk"),
	KUR(new Builder("Kurdish").setIanaCode("ku").setArticles("hende birre")),
	KUS("Kusaie"),
	KUT("Kootenai"),
	LAD("Ladino"),
	LAH("Lahndā"),
	LAM("Lamba (Zambia and Congo)"),
	LAN("Occitan (post 1500)"),
	LAO(new Builder("Lao").setIanaCode("lo")),
	LAP("Sami"),
	LAT(new Builder("Latin").setIanaCode("la")),
	LAV(new Builder("Latvian").setIanaCode("lv")),
	LEZ("Lezgian"),
	LIM(new Builder("Limburgish").setIanaCode("li")
			.setArticles("den dei d' dat dem der daers daeres daer en eng engem enger")),
	LIN(new Builder("Lingala").setIanaCode("ln")),
	LIT(new Builder("Lithuanian").setIanaCode("lt")),
	LOL("Mongo-Nkundu"),
	LOZ("Lozi"),
	LTZ(new Builder("Luxembourgish").setIanaCode("lb")),
	LUA("Luba-Lulua"),
	LUB(new Builder("Luba-Katanga").setIanaCode("lu")),
	LUG(new Builder("Ganda").setIanaCode("lg")),
	LUI("Luiseño"),
	LUN("Lunda"),
	LUO("Luo (Kenya and Tanzania)"),
	LUS("Lushai"),
	MAC(new Builder("Macedonian").setIanaCode("mk")),
	MAD("Madurese"),
	MAG("Magahi"),
	MAH(new Builder("Marshallese").setIanaCode("mh")),
	MAI("Maithili"),
	MAK("Makasar"),
	MAL(new Builder("Malayalam").setIanaCode("ml")),
	MAN("Mandingo"),
	MAO(new Builder("Maori").setIanaCode("mi").setArticles("he nga te")),
	MAP("Austronesian (Other)"),
	MAR(new Builder("Marathi").setIanaCode("mr")),
	MAS("Maasai"),
	MAX("Manx"),
	MAY(new Builder("Malay").setIanaCode("ms")),
	MDF("Moksha"),
	MDR("Mandar"),
	MEN("Mende"),
	MGA("Irish, Middle (ca. 1100-1550)"),
	MIC("Micmac"),
	MIN("Minangkabau"),
	MIS("Miscellaneous languages"),
	MKH("Mon-Khmer (Other)"),
	MLA(new Builder("Malagasy").setIanaCode("mg").setArticles("ny")),
	MLG(new Builder("Malagasy").setIanaCode("mg").setArticles("ny")),
	MLT(new Builder("Maltese").setIanaCode("mt").setArticles("il l")),
	MNC("Manchu"),
	MNI("Manipuri"),
	MNO("Manobo languages"),
	MOH("Mohawk"),
	MOL(new Builder("Moldavian").setIanaCode("ro")), // code mo says ro is preferred
	MON(new Builder("Mongolian").setIanaCode("mn")),
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
	NAP(new Builder("Neapolitan Italian").setArticles("o")),
	NAU(new Builder("Nauru").setIanaCode("na")),
	NAV(new Builder("Navajo").setIanaCode("nv")),
	NBL(new Builder("Ndebele (South Africa)").setIanaCode("nr")),
	NDE(new Builder("Ndebele (Zimbabwe)").setIanaCode("nd")),
	NDO(new Builder("Ndonga").setIanaCode("ng")),
	NDS("Low German"),
	NEP(new Builder("Nepali").setIanaCode("ne")),
	NEW("Newari"),
	NIA("Nias"),
	NIC("Niger-Kordofanian (Other)"),
	NIU(new Builder("Niuean").setArticles("a e taha ha ko")),
	NNO(new Builder("Norwegian (Nynorsk)").setIanaCode("nn").setArticles("ein eit ei")),
	NOB(new Builder("Norwegian (Bokmål)").setIanaCode("nb").setArticles("en et ei")),
	NOG("Nogai"),
	NON("Old Norse"),
	NOR(new Builder("Norwegian").setIanaCode("no")),
	NQO("N'Ko"),
	NSO("Northern Sotho"),
	NUB("Nubian languages"),
	NWC("Newari, Old"),
	NYA(new Builder("Nyanja").setIanaCode("ny")),
	NYM("Nyamwezi"),
	NYN("Nyankole"),
	NYO("Nyoro"),
	NZI("Nzima"),
	OCI(new Builder("Occitan (post-1500)").setIanaCode("oc")
			.setArticles("il l la las le les lh lhi li lis lo los lou lu un una uno uns us")),
	OJI(new Builder("Ojibwa").setIanaCode("oj")),
	ORI(new Builder("Oriya").setIanaCode("or")),
	ORM(new Builder("Oromo").setIanaCode("om")),
	OSA("Osage"),
	OSS(new Builder("Ossetic").setIanaCode("os")),
	OTA("Turkish, Ottoman"),
	OTO("Otomian languages"),
	PAA("Papuan (Other)"),
	PAG("Pangasinan"),
	PAL("Pahlavi"),
	PAM("Pampanga"),
	PAN(new Builder("Panjabi").setIanaCode("pa").setArticles("ال al")),
	PAP("Papiamento"),
	PAU("Palauan"),
	PEO("Old Persian (ca. 600-400 B.C.)"),
	PER(new Builder("Persian").setIanaCode("fa").setArticles("ال al")),
	PHI("Philippine (Other)"),
	PHN("Phoenician"),
	PLI(new Builder("Pali").setIanaCode("pi")),
	POL(new Builder("Polish").setIanaCode("pl")),
	PON("Pohnpeian"),
	POR(new Builder("Portuguese").setIanaCode("pt").setArticles("o a os as um uma")),
	PRA("Prakrit languages"),
	PRO("Provençal (to 1500)"),
	PUS(new Builder("Pushto").setIanaCode("ps")),
	QUE(new Builder("Quechua").setIanaCode("qu")),
	RAJ("Rajasthani"),
	RAP("Rapanui"),
	RAR(new Builder("Rarotongan").setArticles("nga te")),
	ROA("Romance (Other)"),
	ROH(new Builder("Raeto-Romance").setIanaCode("xrr")),
	ROM(new Builder("Romani").setIanaCode("rm")),
	RUM(new Builder("Romanian").setIanaCode("ro").setArticles("un o")),
	RUN(new Builder("Rundi").setIanaCode("rn")),
	RUP("Aromanian"),
	RUS(new Builder("Russian").setIanaCode("ru")),
	SAD("Sandawe"),
	SAG(new Builder("Sango (Ubangi Creole)").setIanaCode("sg")),
	SAH("Yakut"),
	SAI("South American Indian (Other)"),
	SAL("Salishan languages"),
	SAM("Samaritan Aramaic"),
	SAN(new Builder("Sanskrit").setIanaCode("sa")),
	SAO(new Builder("Samoan").setIanaCode("sm").setArticles("le o lo se")),
	SAS("Sasak"),
	SAT("Santali"),
	SCC(new Builder("Serbian").setIanaCode("sr")),
	SCN("Sicilian Italian"),
	SCO(new Builder("Scots").setArticles("a an ane")),
	SCR(new Builder("Croatian").setIanaCode("hr")),
	SEL("Selkup"),
	SEM("Semitic (Other)"),
	SGA("Irish, Old (to 1100)"),
	SGN("Sign languages"),
	SHN("Shan"),
	SHO(new Builder("Shona").setIanaCode("sn")),
	SID("Sidamo"),
	SIN(new Builder("Sinhalese").setIanaCode("si")),
	SIO("Siouan (Other)"),
	SIT("Sino-Tibetan (Other)"),
	SLA("Slavic (Other)"),
	SLO(new Builder("Slovak").setIanaCode("sk")),
	SLV(new Builder("Slovenian").setIanaCode("sl")),
	SMA("Southern Sami"),
	SME(new Builder("Northern Sami").setIanaCode("se")),
	SMI("Sami"),
	SMJ("Lule Sami"),
	SMN("Inari Sami"),
	SMO(new Builder("Samoan").setIanaCode("sm").setArticles("le o lo se")),
	SMS("Skolt Sami"),
	SNA(new Builder("Shona").setIanaCode("sn")),
	SND(new Builder("Sindhi").setIanaCode("sd")),
	SNH("Sinhalese"),
	SNK("Soninke"),
	SOG("Sogdian"),
	SOM(new Builder("Somali").setIanaCode("so")),
	SON("Songhai"),
	SOT(new Builder("Sotho").setIanaCode("st")),
	SPA(new Builder("Spanish").setIanaCode("es").setArticles("el la lo los las un una")),
	SRD(new Builder("Sardinian").setIanaCode("sc")),
	SRN("Sranan"),
	SRP(new Builder("Serbian").setIanaCode("sr")),
	SRR("Serer"),
	SSA("Nilo-Saharan (Other)"),
	SSO(new Builder("Sotho").setIanaCode("st")),
	SSW(new Builder("Swazi").setIanaCode("ss")),
	SUK("Sukuma"),
	SUN(new Builder("Sundanese").setIanaCode("su")),
	SUS("Susu"),
	SUX("Sumerian"),
	SWA(new Builder("Swahili").setIanaCode("sw")),
	SWE(new Builder("Swedish").setIanaCode("sv").setArticles("de den det en ett")),
	SWZ(new Builder("Swazi").setIanaCode("ss")),
	SYC("Syriac"),
	SYR("Syriac, Modern"),
	TAG(new Builder("Tagalog").setArticles("ang mga manga maa")),
	TAH(new Builder("Tahitian").setIanaCode("ty").setArticles("e tahi hui ma maa mau na o pue tau te hoe")),
	TAI("Tai (Other)"),
	TAJ("Tajik"),
	TAM(new Builder("Tamil").setIanaCode("ta")),
	TAR(new Builder("Tatar").setIanaCode("tt")),
	TAT(new Builder("Tatar").setIanaCode("tt")),
	TEL(new Builder("Telugu").setIanaCode("te")),
	TEM("Temne"),
	TER("Terena"),
	TET("Tetum"),
	TGK(new Builder("Tajik").setIanaCode("tg")),
	TGL(new Builder("Tagalog").setIanaCode("tl").setArticles("ang mga manga maa")),
	THA(new Builder("Thai").setIanaCode("th")),
	TIB(new Builder("Tibetan").setIanaCode("bo")),
	TIG("Tigré"),
	TIR(new Builder("Tigrinya").setIanaCode("ti")),
	TIV("Tiv"),
	TKL(new Builder("Tokelauan").setArticles("he ko na ni o te")),
	TLH("Klingon (Artificial language)"),
	TLI("Tlingit"),
	TMH("Tamashek"),
	TOG("Tonga (Nyasa)"),
	TON(new Builder("Tongan").setIanaCode("to").setArticles("he e ko ha koe")),
	TPI("Tok Pisin"),
	TRU("Truk"),
	TSI("Tsimshian"),
	TSN(new Builder("Tswana").setIanaCode("tn")),
	TSO(new Builder("Tsonga").setIanaCode("ts")),
	TSW(new Builder("Tswana").setIanaCode("tn")),
	TUK(new Builder("Turkmen").setIanaCode("tk")),
	TUM("Tumbuka"),
	TUP("Tupi languages"),
	TUR(new Builder("Turkish").setIanaCode("tr").setArticles("al")),
	TUT("Altaic (Other)"),
	TVL("Tuvaluan"),
	TWI(new Builder("Twi").setIanaCode("tw")),
	TYV("Tuvinian"),
	UDM("Udmurt"),
	UGA("Ugaritic"),
	UIG(new Builder("Uighur").setIanaCode("ug")),
	UKR(new Builder("Ukrainian").setIanaCode("uk")),
	UMB("Umbundu"),
	UND("Undetermined"),
	URD(new Builder("Urdu").setIanaCode("ur").setArticles("ال al")),
	UZB(new Builder("Uzbek").setIanaCode("uz")),
	VAI("Vai"),
	VEN(new Builder("Venda").setIanaCode("ve")),
	VIE(new Builder("Vietnamese").setIanaCode("vi")),
	VOL(new Builder("Volapük").setIanaCode("vo")),
	VOT("Votic"),
	WAK("Wakashan languages"),
	WAL("Wolayta"),
	WAR("Waray"),
	WAS("Washoe"),
	WEL(new Builder("Welsh").setIanaCode("cy").setArticles("y yr")),
	WEN("Sorbian (Other)"),
	WLN(new Builder("Walloon").setIanaCode("wa").setArticles("des ein enne l les li")),
	WOL(new Builder("Wolof").setIanaCode("wo")),
	XAL("Oirat"),
	XHO(new Builder("Xhosa").setIanaCode("xh")),
	YAO("Yao (Africa)"),
	YAP("Yapese"),
	YID(new Builder("Yiddish").setIanaCode("yi") //ji also listed for IANA, but with preferred listed as id
			.setArticles("דער der די di דאָס dos דעם dem  אַ a אַן an דער die אן eyn א eyne")),
	YOR(new Builder("Yoruba").setIanaCode("yo")),
	YPK("Yupik languages"),
	ZAP("Zapotec"),
	ZBL("Blissymbolics"),
	ZEN("Zenaga"),
	ZHA(new Builder("Zhuang").setIanaCode("za")),
	ZND("Zande languages"),
	ZUL(new Builder("Zulu").setIanaCode("zu")),
	ZUN("Zuni"),
	ZXX("No linguistic content"),
	ZZA("Zaza");
		
		private String langName;
		private String articles;
		public  String ianaCode;
		private Code(String langName) {
			this.langName = langName;
			this.articles = null;
			this.ianaCode = null;
		}
		private Code(Builder b) {
			this.langName = b.langName;
			this.articles = b.articles;
			this.ianaCode = b.ianaCode;
		}

		public String getLanguageName() { return this.langName; }
		public String getArticles() { return this.articles; }
		public String getIanaCode() {
			if (this.ianaCode != null) return this.ianaCode;
			return this.name().toLowerCase();
		}

		private static class Builder {
			private String langName;
			private String articles;
			private String ianaCode;

			public Builder(String langName) {
				this.langName = langName;
				this.articles = null;
				this.ianaCode = null;
			}

			public Builder setArticles(String articles) { this.articles = articles; return this; }
			public Builder setIanaCode(String ianaCode) { this.ianaCode = ianaCode; return this; }
		}

	}
}
