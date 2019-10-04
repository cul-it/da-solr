package edu.cornell.library.integration.indexer.utilities;

/**
 * The set of MARC relator codes taken directly from
 * &lt;http://www.loc.gov/marc/relators/relacode.html&gt;.
 * Although it is standard to create enum values in all caps, Relator uses the
 * matching capitalization (i.e. all lower case) to what will appear in MARC
 * records.<br/>
 * <br/>
 * Relator r = Relator.valueOf("aut"); <br/>
 * // or <br/>
 * Relator r = Relator.valueOfString("author"); <br/>
 * // r now has value Relator.aut.<br/><br/>
 * r.toString(); // returns "author". <br/>
 * r.name();     // returns "aut". <br/>
 * <br/>
 * Relator r = Relator.valueOf("invalid code"); <br/>
 *   // -> throws IllegalArgumentException
 * Relator r = Relator.valueOfString("invalid term"); <br/>
 *   // -> returns null
 * 
 */
enum Relator {
	abr("abridger"),
	acp("art copyist"),
	act("actor"),
	adi("art director"),
	adp("adapter"),
	aft("author of afterword, colophon, etc."),
	anl("analyst"),
	anm("animator"),
	ann("annotator"),
	ant("bibliographic antecedent"),
	ape("appellee"),
	apl("appellant"),
	app("applicant"),
	aqt("author in quotations or text abstracts"),
	arc("architect"),
	ard("artistic director"),
	arr("arranger"),
	art("artist"),
	asg("assignee"),
	asn("associated name"),
	ato("autographer"),
	att("attributed name"),
	auc("auctioneer"),
	aud("author of dialog"),
	aui("author of introduction, etc."),
	aus("screenwriter"),
	aut("author"),
	bdd("binding designer"),
	bjd("bookjacket designer"),
	bkd("book designer"),
	bkp("book producer"),
	blw("blurb writer"),
	bnd("binder"),
	bpd("bookplate designer"),
	brd("broadcaster"),
	brl("braille embosser"),
	bsl("bookseller"),
	cas("caster"),
	ccp("conceptor"),
	chr("choreographer"),
	clb("collaborator"),
	cli("client"),
	cll("calligrapher"),
	clr("colorist"),
	clt("collotyper"),
	cmm("commentator"),
	cmp("composer"),
	cmt("compositor"),
	cnd("conductor"),
	cng("cinematographer"),
	cns("censor"),
	coe("contestant-appellee"),
	col("collector"),
	com("compiler"),
	con("conservator"),
	cor("collection registrar"),
	cos("contestant"),
	cot("contestant-appellant"),
	cou("court governed"),
	cov("cover designer"),
	cpc("copyright claimant"),
	cpe("complainant-appellee"),
	cph("copyright holder"),
	cpl("complainant"),
	cpt("complainant-appellant"),
	cre("creator"),
	crp("correspondent"),
	crr("corrector"),
	crt("court reporter"),
	csl("consultant"),
	csp("consultant to a project"),
	cst("costume designer"),
	ctb("contributor"),
	cte("contestee-appellee"),
	ctg("cartographer"),
	ctr("contractor"),
	cts("contestee"),
	ctt("contestee-appellant"),
	cur("curator"),
	cwt("commentator for written text"),
	dbp("distribution place"),
	dfd("defendant"),
	dfe("defendant-appellee"),
	dft("defendant-appellant"),
	dgg("degree granting institution"),
	dgs("degree supervisor"),
	dis("dissertant"),
	dln("delineator"),
	dnc("dancer"),
	dnr("donor"),
	dpc("depicted"),
	dpt("depositor"),
	drm("draftsman"),
	drt("director"),
	dsr("designer"),
	dst("distributor"),
	dtc("data contributor"),
	dte("dedicatee"),
	dtm("data manager"),
	dto("dedicator"),
	dub("dubious author"),
	edc("editor of compilation"),
	edm("editor of moving image work"),
	edt("editor"),
	egr("engraver"),
	elg("electrician"),
	elt("electrotyper"),
	eng("engineer"),
	enj("enacting jurisdiction"),
	etr("etcher"),
	evp("event place"),
	exp("expert"),
	fac("facsimilist"),
	fds("film distributor"),
	fld("field director"),
	flm("film editor"),
	fmd("film director"),
	fmk("filmmaker"),
	fmo("former owner"),
	fmp("film producer"),
	fnd("funder"),
	fpy("first party"),
	frg("forger"),
	gis("geographic information specialist"),
	grt("graphic technician"),
	his("host institution"),
	hnr("honoree"),
	hst("host"),
	ill("illustrator"),
	ilu("illuminator"),
	ins("inscriber"),
	inv("inventor"),
	isb("issuing body"),
	itr("instrumentalist"),
	ive("interviewee"),
	ivr("interviewer"),
	jud("judge"),
	jug("jurisdiction governed"),
	lbr("laboratory"),
	lbt("librettist"),
	ldr("laboratory director"),
	led("lead"),
	lee("libelee-appellee"),
	lel("libelee"),
	len("lender"),
	let("libelee-appellant"),
	lgd("lighting designer"),
	lie("libelant-appellee"),
	lil("libelant"),
	lit("libelant-appellant"),
	lsa("landscape architect"),
	lse("licensee"),
	lso("licensor"),
	ltg("lithographer"),
	lyr("lyricist"),
	mcp("music copyist"),
	mdc("metadata contact"),
	med("medium"),
	mfp("manufacture place"),
	mfr("manufacturer"),
	mod("moderator"),
	mon("monitor"),
	mrb("marbler"),
	mrk("markup editor"),
	msd("musical director"),
	mte("metal-engraver"),
	mtk("minute taker"),
	mus("musician"),
	nrt("narrator"),
	opn("opponent"),
	org("originator"),
	orm("organizer"),
	osp("onscreen presenter"),
	oth("other"),
	own("owner"),
	pan("panelist"),
	pat("patron"),
	pbd("publishing director"),
	pbl("publisher"),
	pdr("project director"),
	pfr("proofreader"),
	pht("photographer"),
	plt("platemaker"),
	pma("permitting agency"),
	pmn("production manager"),
	pop("printer of plates"),
	ppm("papermaker"),
	ppt("puppeteer"),
	pra("praeses"),
	prc("process contact"),
	prd("production personnel"),
	pre("presenter"),
	prf("performer"),
	prg("programmer"),
	prm("printmaker"),
	prn("production company"),
	pro("producer"),
	prp("production place"),
	prs("production designer"),
	prt("printer"),
	prv("provider"),
	pta("patent applicant"),
	pte("plaintiff-appellee"),
	ptf("plaintiff"),
	pth("patent holder"),
	ptt("plaintiff-appellant"),
	pup("publication place"),
	rbr("rubricator"),
	rcd("recordist"),
	rce("recording engineer"),
	rcp("addressee"),
	rdd("radio director"),
	red("redaktor"),
	ren("renderer"),
	res("researcher"),
	rev("reviewer"),
	rpc("radio producer"),
	rps("repository"),
	rpt("reporter"),
	rpy("responsible party"),
	rse("respondent-appellee"),
	rsg("restager"),
	rsp("respondent"),
	rsr("restorationist"),
	rst("respondent-appellant"),
	rth("research team head"),
	rtm("research team member"),
	sad("scientific advisor"),
	sce("scenarist"),
	scl("sculptor"),
	scr("scribe"),
	sds("sound designer"),
	sec("secretary"),
	sgd("stage director"),
	sgn("signer"),
	sht("supporting host"),
	sll("seller"),
	sng("singer"),
	spk("speaker"),
	spn("sponsor"),
	spy("second party"),
	srv("surveyor"),
	std("set designer"),
	stg("setting"),
	stl("storyteller"),
	stm("stage manager"),
	stn("standards body"),
	str("stereotyper"),
	tcd("technical director"),
	tch("teacher"),
	ths("thesis advisor"),
	tld("television director"),
	tlp("television producer"),
	trc("transcriber"),
	trl("translator"),
	tyd("type designer"),
	tyg("typographer"),
	uvp("university place"),
	vac("voice actor"),
	vdg("videographer"),
	voc("vocalist"),
	wac("writer of added commentary"),
	wal("writer of added lyrics"),
	wam("writer of accompanying material"),
	wat("writer of added text"),
	wdc("woodcutter"),
	wde("wood engraver"),
	win("writer of introduction"),
	wit("witness"),
	wpr("writer of preface"),
	wst("writer of supplementary textual content");
	
	private String string;
	private Relator(String string) {
		this.string = string;
	}

	static Relator valueOfString( String string ) {
		for ( Relator r : Relator.values() ) {
			if (r.toString().equals(string))
				return r;
		}
		return null;
	}
	@Override public String toString() { return this.string; }
}
