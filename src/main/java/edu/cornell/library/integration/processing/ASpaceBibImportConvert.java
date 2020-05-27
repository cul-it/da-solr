package edu.cornell.library.integration.processing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;

import com.ctc.wstx.exc.WstxEOFException;

import edu.cornell.library.integration.catalog.Catalog;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.metadata.generator.Language;
import edu.cornell.library.integration.metadata.support.Relator;
import edu.cornell.library.integration.utilities.Config;

public class ASpaceBibImportConvert {

	static BufferedWriter updatedBibsFileWriter = null;
	static BufferedWriter newBibsFileWriter = null;

	//TODO 856 protect only when NOT a finding aid link
	public static void main(String[] args)
			throws IOException, XMLStreamException, NumberFormatException, SQLException, InterruptedException, ReflectiveOperationException {
		Collection<String> requiredArgs = Config.getRequiredArgsForDB("Voy");
		requiredArgs.add("catalogClass");

		Config config = Config.loadConfig(requiredArgs);

		String marcDirectory = "C:\\Users\\fbw4\\Documents\\archivespace\\May5";
		Pattern bibIdFileName = Pattern.compile("(\\d+).xml");
		Pattern newBibFileName = Pattern.compile("new(\\d+).xml");

		Catalog.DownloadMARC downloader = Catalog.getMarcDownloader(config);

		StringBuilder allDiffs = new StringBuilder();
		int changedBibCount = 0;
		for (String file : listFilesForFolder(new File(marcDirectory))) {
			MarcRecord newMarc = null;
			try {
				newMarc = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC, readFile(marcDirectory + "\\" + file), true);
			} catch ( WstxEOFException e ) {
				System.out.printf("File %s is not valid XML.\n",file);
				e.printStackTrace();
				continue;
			}
			Matcher m = bibIdFileName.matcher(file);
			if (m.matches()) {
				newMarc.id = m.group(1);
				StringBuilder bibDiffs = processUpdateBib(newMarc, downloader, m.group(1));
				if (bibDiffs != null) {
					changedBibCount++;
					allDiffs.append(bibDiffs);
				}
			} else {
				m = newBibFileName.matcher(file);
				if (m.matches()) {
					processNewBib(newMarc, file);
				} else
					System.out.println("File name not recognized: " + file);
			}

		}
		if ( updatedBibsFileWriter != null ) {
			updatedBibsFileWriter.write("</collection>\n");
			updatedBibsFileWriter.close();
		}
		if ( newBibsFileWriter != null ) {
			newBibsFileWriter.write("</collection>\n");
			newBibsFileWriter.close();
		}
		System.out.printf("\n%d bibs changed from Voyager.\n", changedBibCount);
		System.out.println(allDiffs.toString());
	}


	// NAME FIELDS
	private static void reorderSubfieldsInNames(MarcRecord newMarc) {
		for (DataField f : newMarc.dataFields)
			if (f.tag.endsWith("00")) {
				boolean containsC = false;
				boolean containsQ = false;
				for (Subfield sf : f.subfields) {
					if (sf.code.equals('c'))
						containsC = true;
					if (sf.code.equals('q'))
						containsQ = true;
				}
				if (!containsC || !containsQ)
					continue;
				TreeSet<Subfield> subfieldQSetasides = new TreeSet<>();
				TreeSet<Subfield> newSubfieldOrder = new TreeSet<>();
				boolean seenC = false;
				int sfid = 0;
				for (Subfield sf : f.subfields) {
					if (!seenC && sf.code.equals('q')) {
						subfieldQSetasides.add(sf);
					} else {
						sf.id = ++sfid;
						newSubfieldOrder.add(sf);
						if (sf.code.equals('c')) {
							seenC = true;
							for (Subfield sfq : subfieldQSetasides) {
								sfq.id = ++sfid;
								newSubfieldOrder.add(sfq);
							}
							subfieldQSetasides.clear();
						}
					}
				}
				f.subfields = newSubfieldOrder;
			}
	}
	private static void suppressNoPrimaryCreator(MarcRecord newMarc) {
		DataField toSuppress = null;
		for (DataField f : newMarc.dataFields)
			if (f.tag.startsWith("1"))
				for (Subfield sf : f.subfields)
					if (sf.code.equals('a') && sf.value.toLowerCase().contains("no primary cre"))
						toSuppress = f;
		if (toSuppress != null)
			newMarc.dataFields.remove(toSuppress);
	}
	private static void handleAuthorTitleFields(MarcRecord newMarc) {
		for (DataField f : newMarc.dataFields)
			if (f.tag.endsWith("00") || f.tag.endsWith("10") || f.tag.endsWith("11")) {
				Subfield gsf = null;
				Boolean event = null;
				Set<GSubfield> gExtractedSubfields = null;
				Subfield n = null;
				for (Subfield sf : f.subfields)
					if (sf.code.equals('g')) {
						gExtractedSubfields = mungeNameFieldG(sf);
						gsf = sf;
					} else if ( sf.code.equals('n') )
						n = sf;
					else if ( sf.code.equals('b') )
						event = false;
				if (gsf != null) {
					f.subfields.remove(gsf);
					int id = f.subfields.last().id;
					if (gExtractedSubfields != null) {
						if ( n != null ) {
							f.subfields.remove(n);
							gExtractedSubfields.add(new GSubfield( GPart.n, n.value.replace(").",":") ));
						}
						for (GSubfield newsf : gExtractedSubfields) {
							f.subfields.add(new Subfield(++id, newsf.gpart.name().charAt(0), newsf.value));
							if (newsf.gpart.equals(GPart.c) || newsf.gpart.equals(GPart.d))
								if ( event == null ) event = true;
						}
					}
					// Treaties can have subfields c and d, but are not events
					if (event != null && event && f.tag.endsWith("10") && !f.toString().toLowerCase().contains("treat2"))
						f.tag = f.tag.substring(0, 2) + '1';
				}

			}
	}
	private static Set<GSubfield> mungeNameFieldG(Subfield sf) {
		List<GSubfield> extractedSubfields = new ArrayList<>();
		String value_theInterestingPart = (sf.value.startsWith("(")) ? sf.value.substring(1, sf.value.length() - 3)
				: sf.value.substring(0, sf.value.length() - 1);
		String[] parts = value_theInterestingPart.split(": ");
		GPart prevType = null;
		for (String part : parts) {
			GPart thisType = null;
			String prevValue = null;
			for (GPart p : GPart.values())
				if (part.endsWith(p.getLabel())) {
					thisType = p;
					prevValue = part.substring(0, part.length() - p.getLabel().length());
				}
			if (prevType == null && !extractedSubfields.isEmpty())
				extractedSubfields.get(extractedSubfields.size() - 1).value += ": " + part;
//				System.out.println("null prevType on not-first part "+part);
			if (prevValue == null) {
				prevValue = part;
			}
			if (prevType != null)
				extractedSubfields.add(new GSubfield(prevType, prevValue));
			prevType = thisType;
		}
		Set<GSubfield> sortedNewSubfields = new TreeSet<>();
		sortedNewSubfields.addAll(extractedSubfields);
		return sortedNewSubfields;
	}
	private static class GSubfield implements Comparable<GSubfield> {
		GPart gpart;
		String value;

		public GSubfield(GPart gpart, String value) {
			this.gpart = gpart;
			this.value = value;
		}

		@Override
		public String toString() {
			return String.format("%s: [%s]", this.gpart.name(), this.value);
		}

		@Override
		public int compareTo(GSubfield o) {
			if (!this.gpart.equals(o.gpart))
				return Integer.compare(this.gpart.ordinal(), o.gpart.ordinal());
			return this.value.compareTo(o.value);
		}

		@Override
		public boolean equals(final Object o) {
			if (o == null)
				return false;
			if (!this.getClass().equals(o.getClass()))
				return false;
			GSubfield other = (GSubfield) o;
			return this.gpart.equals(other.gpart) && this.value.equals(other.value);
		}

		@Override
		public int hashCode() {
			return this.toString().hashCode();
		}
	}
	private enum GPart {
		t("Title of work"),
		k("Form subheading"),
		n("Name of a part/section of a work"),
		l("Language of a work"),
		f("Date of work"),
		u("Affiliation"),
		d("Date of meeting or treaty signing"),
		c("Location of meeting"),
		g("Miscellaneous information");

		private final String label;

		private GPart(String label) {
			this.label = label;
		}

		public String getLabel() {
			return this.label;
		}

		@Override
		public String toString() {
			return this.name() + " (" + this.label + ")";
		}
	}
	private static void applyFullRelatorNames(MarcRecord newMarc) {
		for ( DataField f : newMarc.dataFields )
			if ( f.tag.endsWith("00") || f.tag.endsWith("10") || f.tag.endsWith("11") ) {
				Subfield prev = null;
				for ( Subfield sf : f.subfields ) {
					if ( sf.code.equals('4') || sf.code.equals('e') ) {
						Relator r = null;
						try {
							r = Relator.valueOf(sf.value.replaceAll("[^a-z]",""));
						} catch (@SuppressWarnings("unused") IllegalArgumentException e) {
							r = Relator.valueOfString(sf.value.replaceAll("[\\.,]*$", "").toLowerCase());
						}
						if ( r == null ) {
							System.out.printf("Unrecognized relator code: %s (B%s)\n",sf.value,newMarc.controlFields.first().value);
							prev = sf; continue;
						}
						if ( sf.equals(f.subfields.last()) )
							sf.value = r.toString()+'.';
						else
							sf.value = r.toString();
						sf.code = 'e';
						if ( prev != null ) {
							Matcher m = neededTerminalPeriod.matcher(prev.value);
							if ( m.matches() ) {
								prev.value = prev.value.replaceAll("[\\., ]+$", "\\.,");
							} else {
								prev.value = prev.value.replaceAll("[\\., ]+$",",");
								prev.value = prev.value.replaceAll("- ?,$", "-");
							}
						}
					}
					prev = sf;
				}
			}
	}


	// TITLE
	private static void apply245numberOfNonfilingChars(MarcRecord newMarc, MarcRecord oldMarc) {
		Language.Code langCode = null;
		for (ControlField f : newMarc.controlFields)
			if (f.tag.equals("008"))
				langCode = languageCode(f.value.substring(35, 38).toLowerCase());
		for (DataField f : newMarc.dataFields) {
			if (f.tag.equals("245")) {
				int nonFilingChars = calculateNonFilingChars(f, langCode);
				f.ind2 = String.valueOf(nonFilingChars).charAt(0);
				if (oldMarc == null) {
					System.out.println("Generated number of non-filing characters for new record's title. (" + f.ind2 + ")");
					System.out.println(f.toString() + "\n");
				} else
					for (DataField oldf : oldMarc.dataFields)
						if (oldf.tag.equals("245")) {
							String oldTitleStart = oldf.concatenateSubfieldsOtherThan6();
							oldTitleStart = Normalizer
									.normalize(oldTitleStart.replaceAll("[\\[\\]]", ""), Normalizer.Form.NFKD).toLowerCase()
									.replaceAll("\\p{InCombiningDiacriticalMarks}", "");
							if (oldTitleStart.length() > 10)
								oldTitleStart = oldTitleStart.substring(0, 10);
							String aspaceTitleStart = f.concatenateSubfieldsOtherThan6();
							aspaceTitleStart = Normalizer
									.normalize(aspaceTitleStart.replaceAll("[\\[\\]]", ""), Normalizer.Form.NFKD).toLowerCase()
									.replaceAll("\\p{InCombiningDiacriticalMarks}", "");
							if (aspaceTitleStart.length() > 10)
								aspaceTitleStart = aspaceTitleStart.substring(0, 10);
							if (!oldf.ind2.equals(f.ind2)) {
								if (oldTitleStart.equals(aspaceTitleStart)) {
									System.out.println(
											"Mismatch between Voyager and calculated number of non-filing characters in the main title, b"
													+ newMarc.id + " Defaulting to Voyager.");
									System.out.println("Calculated: " + f.toString());
									System.out.println("Voyager:    " + oldf + "\n");
									f.ind2 = oldf.ind2;
								} else {
									System.out.println(
											"Title mismatch in existing record. Using generated number of non-filing characters, b"
													+ newMarc.id);
									System.out.println("Calculated: " + f.toString());
									System.out.println("Voyager:    " + oldf + "\n");
								}
							}
						}
			}
		}

	}
	private static void flip245fg_to264_0(MarcRecord newMarc) {
		String f = null;
		String g = null;
		DataField prodField = null;
		for (DataField df : newMarc.dataFields)
			if (df.tag.equals("245")) {
				for (Subfield sf : df.subfields)
					if (sf.code.equals('f'))
						f = sf.value;
					else if (sf.code.equals('g'))
						g = sf.value;
				String c = getDisplayFormatForDateRanges(f, g);
				if (c == null)
					return;
				Subfield last = null;
				Set<Subfield> removes = new HashSet<>();
				for (Subfield sf : df.subfields)
					if (sf.code.equals('f') || sf.code.equals('g'))
						removes.add(sf);
					else
						last = sf;
				df.subfields.removeAll(removes);
				if (last != null)
					last.value = last.value.replaceAll("\\.?,$", ".");
				if (!c.isEmpty()) {
					TreeSet<Subfield> sfs = new TreeSet<>();
					sfs.add(new Subfield(1, 'c', c));
					prodField = new DataField(assignNewFieldId(newMarc, 264), "264", ' ', '0', sfs);
				}
			}
		if (prodField != null)
			newMarc.dataFields.add(prodField);
	}
	private static String getDisplayFormatForDateRanges(String f, String g) {
		if (f == null && g == null)
			return null;

		List<String> processedFParts = new ArrayList<>();
		List<String> processedGParts = new ArrayList<>();

		if (f != null) {
			f = f.replaceAll("-\\(?bulk", ",bulk");
			String[] fs = f.split(",\\s*");
			boolean bulkFoundInF = false;
			for (String fPart : fs) {
				Matcher m = century.matcher(fPart);
				if (m.matches())
					fPart = m.group(1) + "00s";
				fPart = fPart.replace("n.d.", "undated");
				fPart = fPart.replaceAll("[\\[\\]\\(\\)]", "");
				fPart = fPart.replaceFirst("^-", "");
				fPart = fPart.replaceAll("\\.$", "");
				if (bulkFoundInF || fPart.contains("bulk")) {
					bulkFoundInF = true;
					if (g == null)
						g = fPart;
					else
						g += ", " + fPart;
					continue;
				}
				m = undated.matcher(fPart);
				if (fPart.isEmpty() || m.matches())
					continue;
				String[] dates = fPart.split("\\s*\\-\\s*");
				for (int i = 0; i < dates.length; i++) {
					dates[i] = dates[i].replaceFirst("^ca?[\\.\\s]+", "circa ");
				}
				processedFParts.add(String.join("-", dates));
			}
		}

		if (g != null) {
			String[] gs = g.split(",\\s*");
			for (String gPart : gs) {
				Matcher m = century.matcher(gPart);
				if (m.matches())
					gPart = m.group(1) + "00s";
				gPart = gPart.replace("n.d.", "undated");
				gPart = gPart.replaceAll("[\\[\\]]", "");
				gPart = gPart.replaceFirst("^[\\(-]*", "");
				gPart = gPart.replaceAll("[\\.\\)]*$", "");
				gPart = gPart.replaceAll("\\s*\\(?bulk\\)?\\s*", "");
				m = undated.matcher(gPart);
				if (gPart.isEmpty() || m.matches())
					continue;
				String[] dates = gPart.split("\\s*\\-\\s*");
				for (int i = 0; i < dates.length; i++) {
					dates[i] = dates[i].replaceFirst("^ca?[\\.\\s]+", "circa ");
				}
				processedGParts.add(String.join("-", dates));
			}
		}

		if (processedFParts.isEmpty()) {
			if (processedGParts.isEmpty())
				return "";
			return "bulk " + String.join(", ", processedGParts);
		}
		if (processedGParts.isEmpty())
			return String.join(", ", processedFParts);
		return String.format("%s (bulk %s)", String.join(", ", processedFParts), String.join(", ", processedGParts));

	}// B2070425 2067313
	private static int calculateNonFilingChars(DataField f, Language.Code lang) {
		Set<String> articles = new HashSet<>();
		for (String a : Language.Code.ENG.getArticles().split(" "))
			articles.add(a);
		if (lang != null && lang.getArticles() != null)
			for (String a : lang.getArticles().split(" "))
				articles.add(a);
		String title = f.concatenateSubfieldsOtherThan6();
		for (String a : articles) {
			Pattern p = Pattern.compile(String.format("^([\\[\"]?%s(?!\\.)\\b[^a-z0-9]*).*", a), Pattern.CASE_INSENSITIVE);
			Matcher m = p.matcher(title);
			if (m.find()) {
				return m.group(1).length();
			}
		}
		return 0;
	}
	private static Language.Code languageCode(String code) {
		if (!languagesByCode.containsKey(code))
			return null;
		return languagesByCode.get(code);
	}
	private static Map<String, Language.Code> languagesByCode = new HashMap<>();
	static {
		Arrays.stream(Language.Code.values()).forEach(c -> languagesByCode.put(c.toString().toLowerCase(), c));
	}

	// SUBJECTS
	private static void mergeFastSubjectHeadings(MarcRecord newMarc, MarcRecord oldMarc) {
		Set<DataField> fieldsToAdd = new HashSet<>();
		Set<DataField> fieldsToRemove = new HashSet<>();
		if (oldMarc != null)
			for (DataField f : oldMarc.dataFields)
				if (f.tag.startsWith("6"))
					if (f.ind2.equals('7'))
						for (Subfield sf : f.subfields)
							if (sf.code.equals('2') && sf.value.contains("fast"))
								fieldsToAdd.add(f);
		for (DataField f : newMarc.dataFields) {
			Character needs2ndIndicator = null;
			Subfield sf2 = null;
			if (f.tag.startsWith("6") && f.ind2.equals('7')) {
				for (Subfield sf : f.subfields)
					if (sf.code.equals('2'))
						if (sf.value.contains("fast"))
							fieldsToRemove.add(f);
						else if (sf.value.equals("Library of Congress Subject Headings")) {
							needs2ndIndicator = '0';
							sf2 = sf;
						} else if (sf.value.equals("Source not specified")) {
							needs2ndIndicator = '4';
							sf2 = sf;
						}
				if (needs2ndIndicator != null) {
					f.ind2 = needs2ndIndicator;
					f.subfields.remove(sf2);
				}
			}
		}
		if (!fieldsToRemove.isEmpty())
			for (DataField f : fieldsToRemove)
				newMarc.dataFields.remove(f);
		if (!fieldsToAdd.isEmpty())
			for (DataField f : fieldsToAdd) {
				f.id = assignNewFieldId(newMarc, Integer.valueOf(f.tag));
//				f.id = 1 + findIdOfLastPreviousFieldInNewRecord(newMarc,Integer.valueOf(f.tag));
				newMarc.dataFields.add(f);
			}
	}
	private static void subjectSubdivisionFormatCleanup(MarcRecord newMarc) {
		for (DataField f : newMarc.dataFields) {
			if (!f.tag.startsWith("6"))
				continue;
			Subfield lastSf = null;
			for (Subfield sf : f.subfields) {
				if (sf.value.startsWith(":")) {
					sf.value = sf.value.replaceFirst("^:\\s*", "");
					if (lastSf != null)
						lastSf.value = lastSf.value.replaceAll("\\.$", "");
				}
				lastSf = sf;
			}
		}
	}

	// MERGE OTHER FIELDS
	private static String splitAspace035(MarcRecord newMarc) {
		DataField original = null;
		Set<DataField> splitFields = null;
		String collectionId = null;
		for (DataField f : newMarc.dataFields)
			if (f.tag.equals("035")) {
				int numberOfASubfields = 0;
				for (Subfield sf : f.subfields)
					if (sf.code.equals('a'))
						numberOfASubfields++;
				if (numberOfASubfields < 2)
					continue;
				original = f;
				splitFields = new TreeSet<>();
				int id = f.id;
				for (Subfield sf : f.subfields) {
					splitFields.add(new DataField(id++, "035", f.ind1, f.ind2, "‡" + sf.code + " " + sf.value));
					if (sf.value.startsWith("(CULAspace)")) collectionId = sf.value.substring(11);
				}
			}
		if (original != null && splitFields != null) {
			newMarc.dataFields.remove(original);
			for (DataField f : splitFields)
				newMarc.dataFields.add(f);
		}
		return collectionId;
	}
	private static void insertVariousVoyagerManagedFields(MarcRecord newMarc, MarcRecord oldMarc) {
		for (ControlField f : oldMarc.controlFields) {
			if (f.tag.equals("001")) {
				f.id = 0;
				newMarc.controlFields.add(f);
			}
		}
		FIELD: for (DataField f : oldMarc.dataFields) {
			if (isProtectedField(f.tag)) {
				if (f.tag.equals("035"))
					for (Subfield sf : f.subfields)
						if (sf.value.contains("CULAspace"))
							continue FIELD;
				if (f.tag.equals("264"))
					if (f.ind2.equals('0'))
						continue FIELD;
				if (f.tag.equals("260")) {
					f.tag = "264";
					f.ind2 = '1';
				}
				if (f.tag.equals("856")) {
					boolean findingAid = false;
					for (Subfield sf : f.subfields)
						if ( (sf.code.equals('3') || sf.code.equals('z')) && sf.value.equalsIgnoreCase("Finding aid") )
							findingAid = true;
					if ( findingAid ) for (Subfield sf : f.subfields)
						if ( sf.code.equals('x') && sf.value.equals("aspace_protected") )
							findingAid = false;
					if (findingAid) continue;
				}
				DataField newF = new DataField(assignNewFieldId(newMarc, Integer.valueOf(f.tag)), f.tag, f.ind1, f.ind2,
						f.subfields);
				newMarc.dataFields.add(newF);
			}
		}
	}
	private static boolean isProtectedField(String tag) {
		return tag.equals("035") || tag.equals("260") || tag.equals("264") || tag.equals("541") || tag.equals("336")
				|| tag.equals("337") || tag.equals("338") || tag.equals("362") || tag.equals("730") || tag.equals("740")
				|| (threeDigitNumber.matcher(tag).matches() && Integer.valueOf(tag) >= 856);
	}
	private static Pattern threeDigitNumber = Pattern.compile("\\d{3}");
	private static void unlinkUnprotected880Fields(MarcRecord newMarc) {
		for (DataField f : newMarc.dataFields)
			if (f.tag.equals("880"))
				for (Subfield sf : f.subfields)
					if (sf.code.equals('6'))
						if (sf.value.length() >= 6 && !isProtectedField(sf.value.substring(0, 3)))
							sf.value = (new StringBuilder(sf.value)).replace(4, 6, "00").toString();
	}

	// OTHER CLEAN-UP
	private static void addressCarriageReturnsInFields(MarcRecord mrc) {
		Set<DataField> fieldsToAdd = new HashSet<>();
		Set<DataField> fieldsToRemove = new HashSet<>();
		for (DataField f : mrc.dataFields)
			for (Subfield sf : f.subfields)
				if (sf.value.contains("\n")) {
					if (f.mainTag.equals("351"))
						fieldsToRemove.add(f);
					else if (f.mainTag.startsWith("5") && f.subfields.size() == 1) {
						String[] values = sf.value.split("\\r?\\n\\s*\\r?\\n?");
						int i = f.id;
						for (String value : values)
							fieldsToAdd.add(new DataField(i++, f.mainTag, f.ind1, f.ind2, "‡" + sf.code + value));
						fieldsToRemove.add(f);
					} else {
						System.out.printf("Carriage return in b%s field tag %s\n", mrc.id, f.mainTag);
						System.out.println(f.toString());
						sf.value = sf.value.replaceAll("\\s+", " ");
						System.out.println("Normalized:");
						System.out.println(f.toString() + "\n");
					}
				}
		if (!fieldsToRemove.isEmpty())
			for (DataField f : fieldsToRemove)
				mrc.dataFields.remove(f);
		if (!fieldsToAdd.isEmpty())
			for (DataField f : fieldsToAdd)
				mrc.dataFields.add(f);
	}
	private static void cleanUp546Spacing(MarcRecord mrc) {
		for (DataField f : mrc.dataFields)
			if (f.mainTag.equals("546"))
				for (Subfield sf : f.subfields)
					sf.value = sf.value.replaceAll("\\s+", " ").replaceAll(" \\.$", ".");
	}
	private static void cleanUpEmptyFields( MarcRecord mrc ) {
		List<DataField> fieldsToDelete = new ArrayList<>();
		for ( DataField f : mrc.dataFields ) {
			List<Subfield> subfieldsToDelete = new ArrayList<>();
			for ( Subfield sf : f.subfields )
				if ( sf.value.trim().isEmpty() )
					subfieldsToDelete.add(sf);
			for ( Subfield sf : subfieldsToDelete )
				f.subfields.remove(sf);
			if ( f.subfields.isEmpty() )
				fieldsToDelete.add(f);
		}
		for ( DataField f : fieldsToDelete )
			mrc.dataFields.remove(f);
	}
	private static void flattenUnicodePunctuationAndSpacing( MarcRecord mrc ) {
		for ( DataField f : mrc.dataFields )
			for ( Subfield sf : f.subfields )
				sf.value = sf.value
				.replaceAll("\\t", " ")
				.replaceAll("[‘’]", "'")
				.replaceAll("[“”]", "\"")
				.replaceAll("[—–]", "-");
	}
	private static void moveSubfield2ToEndOfField( MarcRecord mrc ) {
		for ( DataField f : mrc.dataFields ) {
			TreeSet<Subfield> reorderedSubfields = new TreeSet<>();
			TreeSet<Subfield> sf20 = new TreeSet<>();
			for ( Subfield sf : f.subfields ) 
				if ( sf.code.equals('2') || sf.code.equals('0') )
					sf20.add(sf);
				else
					reorderedSubfields.add(sf);
			if ( sf20.isEmpty() ) continue;
			int sfid = reorderedSubfields.last().id;
			for ( Subfield sf : sf20 )
				reorderedSubfields.add(new Subfield(++sfid,sf.code,sf.value));
			f.subfields = reorderedSubfields;
		}
	}


	// PROCESSING FUNCTIONS
	private static void processNewBib(MarcRecord newMarc, String file) throws IOException {
		addressCarriageReturnsInFields(newMarc);
		String collectionId = splitAspace035(newMarc);
		reorderSubfieldsInNames(newMarc);
		flip245fg_to264_0(newMarc);
		apply245numberOfNonfilingChars(newMarc, null);
		mergeFastSubjectHeadings(newMarc, null);
		handleAuthorTitleFields(newMarc);
		suppressNoPrimaryCreator(newMarc);
		subjectSubdivisionFormatCleanup(newMarc);
		cleanUp546Spacing(newMarc);
		cleanUpEmptyFields(newMarc);
		moveSubfield2ToEndOfField(newMarc);
		applyFullRelatorNames(newMarc);

		newMarc.leader = newMarc.leader.substring(0, 5) + 'n' + newMarc.leader.substring(6);
		newMarc.dataFields.add(new DataField(assignNewFieldId(newMarc,899),"899",'1',' ',"‡a culaspacecol"));
		newMarc.dataFields.add(new DataField(assignNewFieldId(newMarc,952),"952",'8',' ',"‡b rmc ‡h "+collectionId));
		if ( newBibsFileWriter == null ) {
			newBibsFileWriter = Files.newBufferedWriter(
					Paths.get("C:\\\\Users\\\\fbw4\\\\Documents\\\\archivespace\\\\Output\\\\new\\\\new_bibs.xml"));
			newBibsFileWriter.write(
					"<?xml version='1.0' encoding='UTF-8'?><collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n");
		}
		newBibsFileWriter.write(newMarc.toXML().replace("<?xml version='1.0' encoding='UTF-8'?>", "")
				.replace(" xmlns=\"http://www.loc.gov/MARC21/slim\"","")+"\n"); 

	}
	private static StringBuilder processUpdateBib(MarcRecord newMarc, Catalog.DownloadMARC downloader, String bibId)
			throws NumberFormatException, IOException, SQLException, InterruptedException {

		try (BufferedWriter writer = Files.newBufferedWriter(
				Paths.get("C:\\\\Users\\\\fbw4\\\\Documents\\\\archivespace\\\\Output\\\\temp\\\\" + bibId + "-1.txt"))) {
			writer.write(newMarc.toString());
		}

		addressCarriageReturnsInFields(newMarc);
		splitAspace035(newMarc);
		reorderSubfieldsInNames(newMarc);
		MarcRecord oldMarc = downloader.getMarc(MarcRecord.RecordType.BIBLIOGRAPHIC, Integer.valueOf(bibId));
		List<String> oldMarcFields = serializeForComparison(oldMarc);
		mergeFastSubjectHeadings(newMarc, oldMarc);
		handleAuthorTitleFields(newMarc);
		flip245fg_to264_0(newMarc);
		apply245numberOfNonfilingChars(newMarc, oldMarc);
		insertVariousVoyagerManagedFields(newMarc, oldMarc);
		unlinkUnprotected880Fields(newMarc);
		suppressNoPrimaryCreator(newMarc);
		subjectSubdivisionFormatCleanup(newMarc);
		cleanUp546Spacing(newMarc);
		cleanUpEmptyFields(newMarc);
		flattenUnicodePunctuationAndSpacing(newMarc);
		moveSubfield2ToEndOfField(newMarc);
		applyFullRelatorNames(newMarc);

		StringBuilder bibDiffs = compareOldAndNewMarc(oldMarcFields, serializeForComparison(newMarc));
		if (bibDiffs == null)
			return null;
		bibDiffs.insert(0, "\nB" + bibId + ":\n");

		if ( updatedBibsFileWriter == null ) {
			updatedBibsFileWriter = Files.newBufferedWriter(
					Paths.get("C:\\\\Users\\\\fbw4\\\\Documents\\\\archivespace\\\\Output\\\\updates\\\\updated_bibs.xml"));
			updatedBibsFileWriter.write(
					"<?xml version='1.0' encoding='UTF-8'?><collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n");
		}
		updatedBibsFileWriter.write(newMarc.toXML().replace("<?xml version='1.0' encoding='UTF-8'?>", "")
				.replace(" xmlns=\"http://www.loc.gov/MARC21/slim\"","")+"\n");
		try (BufferedWriter writer = Files.newBufferedWriter(
				Paths.get("C:\\\\Users\\\\fbw4\\\\Documents\\\\archivespace\\\\Output\\\\temp\\\\" + bibId + "-2.txt"))) {
			writer.write(newMarc.toString());
		}
		return bibDiffs;

	}
	private static List<String> serializeForComparison(MarcRecord marc) {
		marc.leader = "00000" + marc.leader.substring(5, 12) + "00000" + marc.leader.substring(17);
		ControlField five = null;
		for (ControlField f : marc.controlFields)
			if (f.tag.equals("005"))
				five = f;
			else if ( f.tag.equals("008") && f.value.length() > 6 )
				f.value = "000000"+f.value.substring(6);
		if (five != null)
			marc.controlFields.remove(five);
		String recordAsString = Normalizer.normalize(marc.toString(), Normalizer.Form.NFC).replaceAll("&amp;","&");
		return Arrays.asList(recordAsString.split("\n"));
	}
	private static StringBuilder compareOldAndNewMarc(List<String> before, List<String> after) {
		List<String> common = new ArrayList<>();
		for (String b : before)
			for (String a : after)
				if (b.equals(a)) {
					common.add(b);
					common.add(a);
				}
		Map<String, Boolean> diff = new TreeMap<>();
		for (String f : before)
			if (!common.contains(f))
				diff.put(f, false);
		for (String f : after)
			if (!common.contains(f))
				diff.put(f, true);
		if (diff.isEmpty())
			return null;
		StringBuilder sb = new StringBuilder();
		for (Entry<String, Boolean> e : diff.entrySet())
			sb.append((e.getValue()) ? "+ " : "- ").append(e.getKey()).append('\n');
		return sb;
	}
	private static int assignNewFieldId(MarcRecord marc, Integer targetTag) {
		int beforeId = 0;
		int beforeTag = 0;
		int afterId = 0;
		for (DataField f : marc.dataFields) {
			if (targetTag < Integer.valueOf(f.tag)) {
				afterId = f.id;
				break;
			}
			beforeId = f.id;
			beforeTag = Integer.valueOf(f.tag);
		}

		if (afterId == 0)
			return beforeId + 100;
		if (targetTag.equals(beforeTag))
			return beforeId + 1;
		int assignId = beforeId + ((afterId - beforeId) / 2);
		if (afterId == assignId || beforeId == assignId) {
			System.out.printf("ID assignment error for field %d between %d and %d.\n", targetTag, beforeId, afterId);
			System.out.println(marc.toString());
		}
		return assignId;
	}
	public static List<String> listFilesForFolder(final File folder) {
		List<String> files = new ArrayList<>();
		for (final File fileEntry : folder.listFiles())
			if (fileEntry.isDirectory())
				listFilesForFolder(fileEntry);
			else
				files.add(fileEntry.getName());
		return files;
	}
	public static String readFile(String filename) throws IOException {
		return new String(Files.readAllBytes(Paths.get(filename)), StandardCharsets.UTF_8);
	}

	private static Pattern century = Pattern.compile("\\[(\\d\\d)--\\]\\.?");
	private static Pattern undated = Pattern.compile("[Uu]ndated\\.?");
	private static Pattern neededTerminalPeriod = Pattern.compile(".*(Mrs|Inc|Sr|[A-Z])\\.[\\.,]*$");
}
