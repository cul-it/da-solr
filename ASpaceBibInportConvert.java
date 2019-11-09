package edu.cornell.library.integration.indexer.utilities;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;

import edu.cornell.library.integration.indexer.solrFieldGen.Language;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.voyager.DownloadMARC;

public class ASpaceBibInportConvert {

	public static void main(String[] args)
			throws IOException, XMLStreamException, NumberFormatException, ClassNotFoundException,
			SQLException, InterruptedException {
		Collection<String> requiredArgs = Config.getRequiredArgsForDB("Voy");

		Config config = Config.loadConfig(args,requiredArgs);

		String marcDirectory = "C:\\Users\\fbw4\\Documents\\archivespace\\aspace-export";

		DownloadMARC downloader = new DownloadMARC( config );

		BIB: for (String file: listFilesForFolder( new File( marcDirectory ))) {
			MarcRecord newMarc = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC, readFile(marcDirectory+"\\"+file));
			String bibId = file.replace(".xml", "");
			Matcher m = bibIdFileName.matcher(file);
			if (m.matches()) {
				newMarc.id = m.group(1);
				processUpdateBib(newMarc,downloader, m.group(1));
			} else {
				m = newBibFileName.matcher(file);
				if ( m.matches() ) {
					processNewBib(file,m.group(1));
				} else
					System.out.println("File name not recognized: "+file);
			}
			
		}
	}
	private static Pattern bibIdFileName = Pattern.compile("(\\d+).xml");
	private static Pattern newBibFileName = Pattern.compile("new(\\d+).xml");

	private static void processNewBib(String file, String aspaceId) {
		// TODO Auto-generated method stub
		
	}
	private static void processUpdateBib(MarcRecord newMarc, DownloadMARC downloader, String bibId)
			throws NumberFormatException, ClassNotFoundException, IOException, XMLStreamException,
			SQLException, InterruptedException {

		addressCarriageReturnsInFields(newMarc);
		MarcRecord oldMarc = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC,
				downloader.downloadMrc( MarcRecord.RecordType.BIBLIOGRAPHIC, Integer.valueOf(bibId)));
		mergeFastSubjectHeadings(newMarc,oldMarc);
		apply245numberOfNonfilingChars(newMarc,oldMarc);

	}

	private static void apply245numberOfNonfilingChars(MarcRecord newMarc, MarcRecord oldMarc) {
		Language.Code langCode = null;
		for ( ControlField f : newMarc.controlFields )
			if ( f.tag.equals("008") )
				langCode = languageCode( f.value.substring(35,38).toLowerCase() );
		for (DataField f : newMarc.dataFields) {
			if (f.mainTag.equals("245")) {
				int nonFilingChars = calculateNonFilingChars(f,langCode);
				f.ind2 = String.valueOf(nonFilingChars).charAt(0);
				for (DataField oldf : oldMarc.dataFields)
					if (oldf.mainTag.equals("245") ) {
						String oldTitleStart = oldf.concatenateSubfieldsOtherThan6();
						if ( oldTitleStart.length() > 10)
							oldTitleStart = oldTitleStart.substring(0, 10);
						String aspaceTitleStart = f.concatenateSubfieldsOtherThan6();
						if ( aspaceTitleStart.length() > 10)
							aspaceTitleStart = aspaceTitleStart.substring(0, 10);
						if ( oldTitleStart.equals(aspaceTitleStart)
								&& ! oldf.ind2.equals(f.ind2)) {
							System.out.println("Apparent mismatch between Voyager and calculated number of non-filing characters in the main title, b"+newMarc.id
									+" Defaulting to Voyager.");
							System.out.println("Calculated: "+f.toString());
							System.out.println("Voyager:    "+ oldf);
							f.ind2 = oldf.ind2;
						}
					}
			}
		}
		
	}
	private static Language.Code languageCode(String code) {
		if ( ! languagesByCode.containsKey(code) )
			return null;
		return languagesByCode.get(code);
	}
	private static Map<String,Language.Code> languagesByCode = new HashMap<>();
	static {
		Arrays.stream(Language.Code.values()).forEach( c -> languagesByCode.put(c.toString().toLowerCase(),c) );
	}



	private static int calculateNonFilingChars(DataField f, Language.Code lang) {
		Set<String> articles = new HashSet<>();
		for (String a : Language.Code.ENG.getArticles().split(" ")) articles.add(a);
		if ( lang != null && lang.getArticles() != null )
			for (String a : lang.getArticles().split(" ")) articles.add(a);
		String title = f.concatenateSubfieldsOtherThan6();
		for (String a : articles) {
			Pattern p = Pattern.compile(
					String.format("^([\\[\"]?%s(?!\\.)\\b[^a-z0-9]*).*",a), Pattern.CASE_INSENSITIVE);
			Matcher m = p.matcher(title);
			if ( m.find() ) {
//				System.out.println( "group("+m.group(1)+"): "+title);
				return m.group(1).length();
			}
		}
//		System.out.println("no group("+title+")");
		return 0;
	}

	private static void mergeFastSubjectHeadings(MarcRecord newMarc, MarcRecord oldMarc) {
		Set<DataField> fieldsToAdd = new HashSet<>();
		Set<DataField> fieldsToRemove = new HashSet<>();
		for (DataField f : oldMarc.dataFields)
			if (f.tag.startsWith("6") && f.ind2.equals('7'))
				for (Subfield sf : f.subfields)
					if (sf.code.equals('2') && sf.value.contains("fast"))
						fieldsToAdd.add(f);
		for (DataField f : newMarc.dataFields) {
			boolean needsLCSHfix = false;
			Subfield lcsh2sf = null;
			if (f.tag.startsWith("6") && f.ind2.equals('7')) {
				for (Subfield sf : f.subfields)
					if (sf.code.equals('2'))
						if ( sf.value.contains("fast"))
							fieldsToRemove.add(f);
						else if ( sf.value.equals("Library of Congress Subject Headings")) {
							needsLCSHfix = true;
							lcsh2sf = sf;
						}
				if ( needsLCSHfix ) {
					f.ind2 = '0';
					f.subfields.remove(lcsh2sf);
				}
			}
		}
		if ( ! fieldsToRemove.isEmpty() || ! fieldsToAdd.isEmpty() ) {
			System.out.println("-------------------------");
			System.out.println(newMarc.toString());
			System.out.println(oldMarc.toString());
		if ( ! fieldsToRemove.isEmpty() )
			for (DataField f : fieldsToRemove)
				newMarc.dataFields.remove(f);
		if ( ! fieldsToAdd.isEmpty() )
			for (DataField f : fieldsToAdd)
				newMarc.dataFields.add(f);
		System.out.println(newMarc.toString());
		}
	}
	private static void addressCarriageReturnsInFields(MarcRecord mrc) {
		Set<DataField> fieldsToAdd = new HashSet<>();
		Set<DataField> fieldsToRemove = new HashSet<>();
		int i = 99999;
		for (DataField f : mrc.dataFields)
			for (Subfield sf: f.subfields)
				if (sf.value.contains("\n")) {
					if (f.mainTag.equals("351"))
						fieldsToRemove.add(f);
					else if ( f.mainTag.startsWith("5") && f.subfields.size() == 1) {
						String[] values = sf.value.split("\\r?\\n\\s*\\r?\\n?");
						for (String value : values)
							fieldsToAdd.add(new DataField(i++,f.mainTag,f.ind1,f.ind2,"‡"+sf.code+value));
						fieldsToRemove.add(f);
					} else
						System.out.printf("carriage return in b%s field tag %s\n",mrc.id,f.mainTag);
				}
		if ( ! fieldsToRemove.isEmpty() )
			for (DataField f : fieldsToRemove)
				mrc.dataFields.remove(f);
		if ( ! fieldsToAdd.isEmpty() )
			for (DataField f : fieldsToAdd)
				mrc.dataFields.add(f);
	}

	public static List<String> listFilesForFolder(final File folder) {
		List<String> files = new ArrayList<>();
		for (final File fileEntry : folder.listFiles()) {
	        if (fileEntry.isDirectory()) {
	            listFilesForFolder(fileEntry);
	        } else {
	        	files.add(fileEntry.getName());
	        }
	    }
		return files;
	}
	public static String readFile ( String filename ) throws IOException {
		return new String(Files.readAllBytes(Paths.get(filename)), StandardCharsets.UTF_8);
	}


}
