package edu.cornell.library.integration.metadata.support;

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;

import edu.cornell.library.integration.catalog.Catalog;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.metadata.generator.Language;
import edu.cornell.library.integration.metadata.generator.Language.Code;
import edu.cornell.library.integration.utilities.Config;

public class CompareMARCWithVoyager {

	public static void main(String[] args)
			throws IOException, XMLStreamException, SQLException, InterruptedException {

		Collection<String> requiredArgs = Config.getRequiredArgsForDB("Voy");
		requiredArgs.add("catalogClass");

		Config config = Config.loadConfig(requiredArgs);

		String marcDirectory = "C:\\Users\\fbw4\\Documents\\archivespace\\dec20bibs\\export-marc";

		Catalog.DownloadMARC downloader = Catalog.getMarcDownloader(config);

		Map<String,List<String>> notePatterns = new HashMap<>();
		Map<String,Integer> counts = new HashMap<>();
		BIB: for ( String file : listFilesForFolder( new File( marcDirectory ) ) ) {
			if ( ! file.endsWith(".xml") ) continue;
			MarcRecord newMarc = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC, readFile(marcDirectory+"\\"+file), true);
			for (DataField f : newMarc.dataFields)
				if (f.mainTag.equals("856"))
					continue BIB;
			String bibId = file.replace(".xml", "");
			MarcRecord oldMarc = downloader.getMarc( MarcRecord.RecordType.BIBLIOGRAPHIC, Integer.valueOf(bibId));
			System.out.println(" *********************** bib: "+bibId+ " ********************");
			System.out.println("voy: "+oldMarc.toString());
			System.out.println("as:  "+newMarc.toString());
			for ( ControlField f : oldMarc.controlFields )
				if ( counts.containsKey(f.tag) )
					counts.put(f.tag, counts.get(f.tag)+1);
				else
					counts.put(f.tag, 1);
			for ( DataField f : oldMarc.dataFields )
				if ( counts.containsKey(f.tag) )
					counts.put(f.tag, counts.get(f.tag)+1);
				else
					counts.put(f.tag, 1);

			Language.Code newLang = null, oldLang = null;
			for ( ControlField f : oldMarc.controlFields ) 
				if ( f.tag.equals("008") )
					oldLang = languageCode( f.value.substring(35,38).toLowerCase() );
			for ( ControlField f : newMarc.controlFields )
				if ( f.tag.equals("008") ) //TODO adjust substring numbers for corrected extract when applicable
					newLang = languageCode( f.value.substring(36,39).toLowerCase() );
			System.out.printf("%s Language: %s %s%s\n",bibId,
					(oldLang==null)?"null":oldLang.getLanguageName(),
							(newLang==null)?"null":newLang.getLanguageName(),
									Objects.equals(oldLang, newLang)?"":" XX");

			Iterator<DataField> newI = newMarc.dataFields.iterator();
			Iterator<DataField> oldI = oldMarc.dataFields.iterator();
			DataField newF = newI.next();
			DataField oldF = oldI.next();
			Map<String,List<String>> newNotes = new HashMap<>();
			Map<String,List<String>> oldNotes = new HashMap<>();
			StringBuilder notePattern = new StringBuilder();
			while ( newF != null && oldF != null ) {
				if ( newF.toString().contains("\n"))
					System.out.printf( "Carriage return in %s field, bib %s\n",newF.mainTag,bibId);
				if ( oldF.mainTag.startsWith("1") )
					for (Subfield sf : oldF.subfields) if (sf.code.equals('f'))
						System.out.printf("‡f in main author entry, bib %s: %s\n",bibId,oldF.toString());
	//			while ( newI.hasNext() && oldI.hasNext() ) {
				if ( newF.mainTag.equals("035") ) {
					newF = (newI.hasNext())?newI.next():null;
					continue;
				}
				if ( oldF.mainTag.equals("035") ) {
					oldF = (oldI.hasNext())?oldI.next():null;
					continue;
				}
				if ( newF.mainTag.startsWith("5") ) {
					if ( ! newNotes.containsKey(newF.mainTag) )
						newNotes.put( newF.mainTag, new ArrayList<String>() );
					newNotes.get(newF.mainTag).add(newF.toString());
					newF = (newI.hasNext())?newI.next():null;
					continue;
				}
				if ( oldF.mainTag.startsWith("5") ) {
					if ( ! oldNotes.containsKey(oldF.mainTag) )
						oldNotes.put( oldF.mainTag, new ArrayList<String>() );
					oldNotes.get(oldF.mainTag).add(oldF.toString());
					if (notePattern.length() > 0 ) notePattern.append(' ');
					notePattern.append(oldF.mainTag);
					oldF = (oldI.hasNext())?oldI.next():null;
					continue;
				}
				if ( newF.mainTag.startsWith("6") && newF.ind2.equals('7') )
					for ( Subfield sf : newF.subfields )
						if ( sf.code.equals('2') )
							if (sf.value.equals("Library of Congress Subject Headings")) {
								newF.ind2 = '0';
								newF.subfields.remove(sf);
							} else if (sf.value.equals("Source not specified")) {
								newF.subfields.remove(sf);
							}
				if ( newF.mainTag.equals( oldF.mainTag ) ) {
					String newField = newF.toString();
					String oldField = oldF.toString();
					if ( newF.mainTag.equals("245") ) {
						 // don't compare first indicator
						newField = newField.substring(5);
						oldField = oldField.substring(5);
						int calculatedNonFiling = calculateNonFilingChars( newF, newLang );
						String calculated2ndIndicator = String.valueOf(calculatedNonFiling);
						if ( calculated2ndIndicator.length() > 1
								|| ! oldF.ind2.equals(calculated2ndIndicator.charAt(0)) ) {
							newF.ind2 = calculated2ndIndicator.charAt(0);
							System.out.printf("2nd indicator mismatch\t%s\t%s\n",
									oldF.toString(), newF.toString());
						}
					} else if ( newF.mainTag.equals("100")) {
						// don't flag the addition of ‡e creator.
						if (newField.endsWith(", ‡e creator.") && ! oldField.endsWith(", ‡e creator."))
							newField = newField.replaceAll(", ‡e creator.", "");
					}
					if ( newF.mainTag.equals("300") ) {
						String newFieldWithoutF = newField.replace(" ‡f", "");
						if ( ! newFieldWithoutF.equals(oldField) )
							System.out.println("b"+bibId+"\t"+oldField+"\t=> "+newField);
					} else if ( ! newField.equals(oldField) ) {
						System.out.println("voy: "+oldF.toString());
						System.out.println("as:  "+newF.toString());
						System.out.println("");
					}
					newF = (newI.hasNext())?newI.next():null;
					oldF = (oldI.hasNext())?oldI.next():null;
				} else if ( Integer.valueOf(newF.mainTag) < Integer.valueOf(oldF.mainTag) ) {
					System.out.println("as:  "+newF.toString());
					System.out.println("");
					newF = (newI.hasNext())?newI.next():null;
				} else {
					System.out.println("voy: "+oldF.toString());
					System.out.println("");
					oldF = (oldI.hasNext())?oldI.next():null;
				}
			}
			while ( newF != null ) {
				if ( newF.mainTag.startsWith("5") ) {
					if ( ! newNotes.containsKey(newF.mainTag) )
						newNotes.put( newF.mainTag, new ArrayList<String>() );
					newNotes.get(newF.mainTag).add(newF.toString());
					newF = (newI.hasNext())?newI.next():null;
					continue;
				}
				System.out.println("as:  "+newF.toString());
				System.out.println("");
				newF = (newI.hasNext())?newI.next():null;
			}
			while ( oldF != null ) {
				if ( oldF.mainTag.startsWith("5") ) {
					if ( ! oldNotes.containsKey(oldF.mainTag) )
						oldNotes.put( oldF.mainTag, new ArrayList<String>() );
					oldNotes.get(oldF.mainTag).add(oldF.toString());
					if (notePattern.length() > 0 ) notePattern.append(' ');
					notePattern.append(oldF.mainTag);
					oldF = (oldI.hasNext())?oldI.next():null;
					continue;
				}
				System.out.println("voy: "+oldF.toString());
				System.out.println("");
				oldF = (oldI.hasNext())?oldI.next():null;
			}
			{
				String s = notePattern.toString();
				if ( ! notePatterns.containsKey(s) ) 
					notePatterns.put(s, new ArrayList<>());
				notePatterns.get(s).add(bibId);
				System.out.println("note fields: "+s);
			}
			{ //compare saved notes fields
				Set<String> noteTags = new TreeSet<>();
				noteTags.addAll(oldNotes.keySet());
				noteTags.addAll(newNotes.keySet());
				for ( String tag : noteTags ) {
					compareFieldsAsStrings(
							((oldNotes.containsKey(tag))?oldNotes.get(tag):null),
							((newNotes.containsKey(tag))?newNotes.get(tag):null)  );
				}
			}
		}
		System.out.println(counts);
/*		for ( Entry<String,List<String>> e : notePatterns.entrySet()) {
			System.out.printf( "%4d\t%s\t%s\t%s\n",
					e.getValue().size(), isObviouslyStructured( e.getKey() ), e.getKey(), String.join(", ",e.getValue()));
		}*/
	}

	private static Code languageCode(String code) {
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

	private static boolean isObviouslyStructured(String pattern) {
		Set<String> appearedFields = new HashSet<>();
		String lastField = "";
		for (String field : pattern.split(" ")) {
			if ( appearedFields.contains(field) && ! lastField.equals(field) )
				return true;
			appearedFields.add(field);
			lastField = field;
		}
		return false;
	}

	public static String readFile ( String filename ) throws IOException {
		return new String(Files.readAllBytes(Paths.get(filename)), StandardCharsets.UTF_8);
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

	public static void compareFieldsAsStrings ( List<String> voyFields, List<String> asFields) {

		if ( asFields == null  ) {
			if ( voyFields != null )
				for ( String voyField : voyFields )
					System.out.println("voy: "+voyField);
			return;
		}
		if ( voyFields == null ) {
			if ( asFields != null )
				for ( String asField : asFields )
					System.out.println("as:  "+asField);
			return;
		}
		Iterator<String> voyIter = voyFields.iterator();
		Iterator<String>  asIter =  asFields.iterator();
		String voyField = voyIter.next();
		String asField  = asIter.next();
		while ( voyField != null && asField != null ) {
			if ( ! voyField.equals(asField) ) {
				if ( ! voyField.substring(5).equals(asField.substring(5))) {
					System.out.println("voy: "+voyField);
					System.out.println("as:  "+asField);
				} else {
					System.out.printf("%s 1st indicator\t%s\t%s\t%s\n",
							voyField.substring(0, 3),
							voyField.substring(4, 5),
							asField.substring(4, 5),
							asField.substring(7));
				}
			}
			voyField = (voyIter.hasNext())? voyIter.next(): null;
			asField  = (asIter.hasNext()) ? asIter.next() : null;
		}
		while ( voyField != null ) {
			System.out.println("voy: "+voyField);
			voyField = (voyIter.hasNext())? voyIter.next(): null;
		}
		while ( asField != null ) {
			System.out.println("as:  "+asField);
			asField  = (asIter.hasNext()) ? asIter.next() : null;
		}
		
	}
}
