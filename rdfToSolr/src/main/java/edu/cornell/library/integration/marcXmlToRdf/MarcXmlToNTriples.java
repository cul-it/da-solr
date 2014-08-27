package edu.cornell.library.integration.marcXmlToRdf;

import static edu.cornell.library.integration.ilcommons.util.CharacterSetUtils.hasCJK;
import static edu.cornell.library.integration.ilcommons.util.CharacterSetUtils.isCJK;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.apache.commons.io.FileUtils;

import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.ControlField;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.RecordType;
import edu.cornell.library.integration.indexer.MarcRecord.Subfield;

//TODO: The coding for individual files as src or dest material is 
// incomplete and untested where it exists.
public class MarcXmlToNTriples {
	
	private static Boolean debug = true;
	
	private static String logfile = "xmltordf.log";
	private static String extractfile = "extract.tdf";
	private static BufferedWriter logout;
	private static BufferedWriter extractout;
	public static Collection<Integer> foundRecs = new HashSet<Integer>();
	public static Collection<Integer> unsuppressedRecs = new HashSet<Integer>();
	public static Map<String,FieldStats> fieldStatsByTag = new HashMap<String,FieldStats>();
	public static Long recordCount = new Long(0);
	public static Collection<Integer> no245a = new HashSet<Integer>();
	private static Integer groupsize = 1000;
	
	private Mode mode;
	private Boolean isUnsuppressedBibListFiltered = false;
	private Boolean isUnsuppressedBibListFlagged = false;
	private Collection<Integer> unsuppressedBibs = null;
	private Boolean isUnsuppressedMfhdListFiltered = false;
	private Boolean isUnsuppressedMfhdListFlagged = false;
	private Collection<Integer> unsuppressedMfhds = null;
	private Boolean isBibSrcDav = null;
	private Boolean isMfhdSrcDav = null;
	private Boolean isDestDav = null;
	private String bibSrcDir = null;
	private String bibSrcFile = null;
	private String mfhdSrcDir = null;
	private String mfhdSrcFile = null;
	private String destDir = null;
	private String destFile = null;
	private String destFilenamePrefix = null;
	private DavService bibSrcDav = null;
	private DavService mfhdSrcDav = null;
	private DavService destDav = null;
	private String tempBibSrcDir = null;
	private String tempDestDir = null;
	private String currentFileName = null;
	private Collection<Report> reports = new HashSet<Report>();
	private Map<Report,String> reportResults = new HashMap<Report,String>();
	private Map<Integer, BufferedOutputStream> outsById = null;
	private Map<String, BufferedOutputStream> outsByName = null;
	private Boolean processingBibs = null;
	private Boolean processingMfhds = null;
	private String uriPrefix = null;
	
	
	public MarcXmlToNTriples(MarcXmlToNTriples.Mode m) {
		mode = m;
	}


	/**
	 * @param Collection<Integer> ids : Collection of Integer record IDs for unsuppressed Bibs.
	 * @param Boolean filter : Should suppressed records be filtered out?
	 * @param Boolean flag : Should suppressed/unsuppressed status be reflected in N-Triples?
	 */
	public void setUnsuppressedBibs( Collection<Integer> ids, Boolean filter, Boolean flag ) {
		if ( ! filter && ! flag )
			throw new IllegalArgumentException("At least one of filter and flag must be true.");
		isUnsuppressedBibListFiltered = filter;
		isUnsuppressedBibListFlagged = flag;
		unsuppressedBibs = ids;
	}
	/**
	 * @param Collection<Integer> ids : Collection of Integer record IDs for unsuppressed Mfhds.
	 * @param Boolean filter : Should suppressed records be filtered out?
	 * @param Boolean flag : Should suppressed/unsuppressed status be reflected in N-Triples?
	 */
	public void setUnsuppressedMfhds( Collection<Integer> ids, Boolean filter, Boolean flag ) {
		if ( ! filter && ! flag )
			throw new IllegalArgumentException("At least one of filter and flag must be true.");
 		isUnsuppressedMfhdListFiltered = filter;
		isUnsuppressedMfhdListFlagged = flag;
		unsuppressedMfhds = ids;
	}
	/**
	 * Set local directory as source of XML bib records.
	 * @param dir : path name (relative or absolute)
	 */
	public void setBibSrcDir( String dir ) {
		if (dir.startsWith("http")) {
			System.out.println("Error: If bibSrcDir is a URL, use setBibSrcDavDir(String, DavService).");
			throw new IllegalArgumentException("If bibSrcDir is a URL, use setBibSrcDavDir(String, DavService).");
		}
		bibSrcDir = dir;
		isBibSrcDav = false;
	}
	/**
	 * Set webdav directory of XML bib records.
	 * @param dir : Directory location as full URL.
	 * @param d : DavService instance with appropriate credentials loaded.
	 */
	public void setBibSrcDavDir( String dir, DavService d ) {
		if (! dir.startsWith("http")) {
			System.out.println("Error: A webdav source directory must start with \"http\".");
			throw new IllegalArgumentException("A webdav source directory must start with \"http\".");
		}
		bibSrcDir = dir;
		isBibSrcDav = true;
		bibSrcDav = d;
	}


	/**
	 * Set local directory as source of XML mfhd records.
	 * @param dir : path name (relative or absolute)
	 */
	public void setMfhdSrcDir( String dir ) {
		if (dir.startsWith("http")) {
			System.out.println("Error: If mfhdSrcDir is a URL, use setMfhdSrcDavDir(String, DavService).");
			throw new IllegalArgumentException("If mfhdSrcDir is a URL, use setMfhdSrcDavDir(String, DavService).");
		}
		mfhdSrcDir = dir;
		isMfhdSrcDav = false;
	}
	/**
	 * Set webdav directory of XML mfhd records.
	 * @param dir : Directory location as full URL.
	 * @param d : DavService instance with appropriate credentials loaded.
	 */
	public void setMfhdSrcDavDir( String dir, DavService d ) {
		if (! dir.startsWith("http")) {
			System.out.println("Error: A webdav source directory must start with \"http\".");
			throw new IllegalArgumentException("A webdav source directory must start with \"http\".");
		}
		mfhdSrcDir = dir;
		isMfhdSrcDav = true;
		mfhdSrcDav = d;
	}

	
	/**
	 * Set local directory as destination for N-Triples (nt.gz) files.
	 * @param dir : path name (relative or absolute)
	 */
	public void setDestDir( String dir ) {
		if (dir.startsWith("http")) {
			System.out.println("Error: If destDir is a URL, use setDestDavDir(String, DavService).");
			throw new IllegalArgumentException("If destDir is a URL, use setDestDavDir(String, DavService).");
		}
		destDir = dir;
		isDestDav = false;
	}
	/**
	 * Set webdav directory for N-Triples files to go.
	 * @param dir : Directory location as full URL.
	 * @param d : DavService instance with appropriate credentials loaded.
	 */
	public void setDestDavDir( String dir, DavService d ) {
		if (! dir.startsWith("http")) {
			System.out.println("Error: A webdav source directory must start with \"http\".");
			throw new IllegalArgumentException("A webdav source directory must start with \"http\".");
		}
		destDir = dir;
		isDestDav = true;
		destDav = d;
	}
	
	/**
	 * Set prefix for filenames in the destination directory.
	 * after prefix will appear ".N.nt.gz" where N is a batch id.
	 * @param pref : Filename prefix
	 */
	public void setDestFilenamePrefix( String pref ) {
		destFilenamePrefix = pref;
	}
	
	public void setUriPrefix( String p ) {
		if (! p.endsWith("/"))
			p += "/";
		uriPrefix = p;
	}
	
	/**
	 * add report to desired reports
	 * @param r
	 */
	public void addReport( Report r ) {
		reports.add(r);
	}
	
	public String getReport( Report r ) {
		if (reportResults.containsKey(r)) 
			return reportResults.get(r);
		return null;
	}
	
	public void run() throws Exception {

		validateRun();
		
		if (mode.equals(Mode.NAME_AS_SOURCE))
			outsByName = new HashMap<String,BufferedOutputStream>();
		else
			outsById = new HashMap<Integer, BufferedOutputStream>();
		
		// If the destination is local, we can build N-Triples directly to there.
		// Otherwise, we need a temporary build directory.
		if (isDestDav) {
			tempDestDir = Files.createTempDirectory("IL-xml2NT-").toString();
			if (debug) System.out.println(tempDestDir);
		}
		
		// If we want RECORD_COUNT_BATCHES, we will need to process bibs twice,
		// so downloading in advance will help.
		if (mode.equals(Mode.RECORD_COUNT_BATCHES) && isBibSrcDav)
			tempBibSrcDir = downloadBibsToTempDir();
		
		// ID_RANGE_BATCHES don't require precalculated ranges, but RECORD_COUNT_BATCHES do.
		if (mode.equals(Mode.RECORD_COUNT_BATCHES))
			determineTargetBatches();
		
		if (processingBibs) processBibs();
		if (reports.contains(Report.GEN_FREQ_BIB))
			reportResults.put(Report.GEN_FREQ_BIB,buildGenFreqReport());

		if (processingMfhds) processMfhds();
		if (reports.contains(Report.GEN_FREQ_MFHD))
			reportResults.put(Report.GEN_FREQ_MFHD,buildGenFreqReport());
		
		if (outsByName != null)
			for (BufferedOutputStream out : outsByName.values())
				out.close();
		if (outsById != null)
			for (BufferedOutputStream out : outsById.values())
				out.close();

		if (isDestDav) uploadNT();
		
		if (tempDestDir != null)
			FileUtils.deleteDirectory(new File(tempDestDir));
		if (tempBibSrcDir != null)
			FileUtils.deleteDirectory(new File(tempBibSrcDir));
		
	}
	
	private void uploadNT() throws Exception {
		
		DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(tempDestDir));
		for (Path file: stream) {
			System.out.println(file.getFileName());
			String targetNTFile = 
					(destDir != null) ? destDir +"/"+file.getFileName()
							: destFile;
			InputStream is = new FileInputStream(file.toString());
			destDav.saveFile(targetNTFile, is);						
			System.out.println("MARC N-Triples file saved to " + targetNTFile);
		}

	}

	private void sortNt( String bibid, String nt ) throws Exception {
		BufferedOutputStream out = null;
		String dirToProcessInto = null;
		
		if (tempDestDir != null)
			dirToProcessInto = tempDestDir;
		else if (destDir != null && isDestDav.equals(false))
			dirToProcessInto = destDir;
		
		if (mode.equals(Mode.ID_RANGE_BATCHES)) {
			Integer batchid = Integer.valueOf(bibid) / groupsize;
			if ( ! outsById.containsKey(batchid)) {
				out =  new BufferedOutputStream(new GZIPOutputStream(
						new FileOutputStream(dirToProcessInto+"/"+
								destFilenamePrefix+"."+batchid+".nt.gz", true)));
				outsById.put(batchid, out);
			} else 
				out = outsById.get(batchid);

		} else if (mode.equals(Mode.RECORD_COUNT_BATCHES)) {
			Integer outputBatch = 100_000_000;
			Iterator<Integer> i = outsById.keySet().iterator();
			Integer id = Integer.valueOf(bibid);
			while (i.hasNext()) {
				Integer batch = i.next();
				if ((id <= batch) && (outputBatch > batch))
					outputBatch = batch;
			}
			if (outputBatch == 100_000_000) {
				System.out.println("Failed to identify output batch for bib "+bibid
						+". Not writing record to N-Triples.");
			}
			out = outsById.get(outputBatch);

		} else { //Mode.NAME_AS_SOURCE
			if (outsByName.containsKey(currentFileName)) {
				out = outsByName.get(currentFileName);
			} else {
				if (dirToProcessInto != null) {
					String targetFile = dirToProcessInto+"/"+swapFileExt(currentFileName);
					if (debug)
						System.out.println("Opening output handle for "+targetFile);
					out = new BufferedOutputStream(new GZIPOutputStream(
							new FileOutputStream(targetFile)));
				} else {
					// dest is a file, and not on dav
					out = new BufferedOutputStream(new GZIPOutputStream(
							new FileOutputStream(dirToProcessInto+"/"+
									swapFileExt(currentFileName))));
				}
				outsByName.put(currentFileName, out);
			}
		}
		
		if (out != null) {
			out.write( nt.getBytes() );
		} else {
			System.out.println("N-Triples not written to file. Bibid: "+bibid);
		}
	}
	
	public String swapFileExt ( String xml ) {
		String nt;
		if (xml.endsWith(".xml"))
			nt = xml.substring(0,xml.length()-4)+".nt.gz";
		else
			nt = xml + ".nt.gz";
		return nt;
	}
	
	private void readXml( InputStream xmlstream, RecordType type ) throws XMLStreamException, Exception {
		XMLInputFactory input_factory = XMLInputFactory.newInstance();
		XMLStreamReader r  = 
				input_factory.createXMLStreamReader(xmlstream);
		while (r.hasNext()) {
			String event = getEventTypeString(r.next());
			if (event.equals("START_ELEMENT"))
				if (r.getLocalName().equals("record")) {
					MarcRecord rec = processRecord(r);
					rec.type = type;

					if (isSuppressionBlocked(rec.id, type))
						continue;

					mapNonRomanFieldsToRomanizedFields(rec);
					if (reports.contains(Report.QC_CJK_LABELING))
						surveyForCJKValues(rec);
					if ((type.equals(RecordType.BIBLIOGRAPHIC) && reports.contains(Report.GEN_FREQ_BIB))
							|| (type.equals(RecordType.HOLDINGS) && reports.contains(Report.GEN_FREQ_MFHD))) {
						tabulateFieldData(rec);
					}
					String ntriples = generateNTriples( rec, type );
					if (type.equals(RecordType.BIBLIOGRAPHIC))
						sortNt(rec.id,ntriples);
					else if (type.equals(RecordType.HOLDINGS))
						sortNt(rec.bib_id,ntriples);
				}
		}
		r.close();
		xmlstream.close();
	}
	
	private Boolean isSuppressionBlocked(String id, RecordType type) {
		if (type.equals(RecordType.BIBLIOGRAPHIC)
				&& isUnsuppressedBibListFiltered
				&& ! unsuppressedBibs.contains(Integer.valueOf(id)))
			return true;
		if (type.equals(RecordType.HOLDINGS)
				&& isUnsuppressedMfhdListFiltered
				&& ! unsuppressedMfhds.contains(Integer.valueOf(id)))
			return true;
		return false;
	}
	
	public static String escapeForNTriples( String s ) {
		s = s.replaceAll("\\\\", "\\\\\\\\");
		s = s.replaceAll("\"", "\\\\\\\"");
		s = s.replaceAll("[\n\r]+", "\\\\n");
		s = s.replaceAll("\t","\\\\t");
		return s;
	}
	
	private void processBibs(  ) throws Exception {
		
		String localProcessDir = null;
		String localProcessFile = null;
		if (tempBibSrcDir != null)
			localProcessDir = tempBibSrcDir;
		if ( ! isBibSrcDav )
			if ( bibSrcDir != null ) 
				localProcessDir = bibSrcDir;
			else
				localProcessFile = bibSrcFile;
		
		if (localProcessDir != null) {
			DirectoryStream<Path> stream = Files.newDirectoryStream(
					Paths.get(localProcessDir));
			for (Path file: stream) {
				currentFileName = file.toString().substring(
						file.toString().lastIndexOf(File.separator)+1);
				if (debug) System.out.println(file + " ("+currentFileName+")");
				readXml(new FileInputStream(file.toString()),
						RecordType.BIBLIOGRAPHIC );
			}
			return;
		}
		
		if (localProcessFile != null) {
			currentFileName = localProcessFile.substring(
					localProcessFile.lastIndexOf(File.separator)+1);
			if (debug) System.out.println(localProcessFile + " ("+currentFileName+")");
			readXml(new FileInputStream(localProcessFile),
					RecordType.BIBLIOGRAPHIC );
			return;
		}
		
		// At this point we're looking for Dav sources
		if (bibSrcDir != null) {
			List<String> files = bibSrcDav.getFileUrlList(bibSrcDir);
			for ( String file : files) {
				currentFileName = file.substring(file.lastIndexOf('/')+1);
				if (debug) System.out.println(file + " ("+currentFileName+")");
				readXml(bibSrcDav.getFileAsInputStream(file),
						RecordType.BIBLIOGRAPHIC );
			}
			return;
		}
		
		if (bibSrcFile != null) {
			currentFileName = bibSrcFile.substring(bibSrcFile.lastIndexOf('/')+1);
			if (debug) System.out.println(bibSrcFile + " ("+currentFileName+")");
			readXml(bibSrcDav.getFileAsInputStream(bibSrcFile),
					RecordType.BIBLIOGRAPHIC );
			return;
		}		
		
	}
	
	private void processMfhds( ) throws Exception {
		String localProcessDir = null;
		String localProcessFile = null;

		if ( ! isMfhdSrcDav )
			if ( mfhdSrcDir != null ) 
				localProcessDir = mfhdSrcDir;
			else
				localProcessFile = mfhdSrcFile;
		
		if (localProcessDir != null) {
			DirectoryStream<Path> stream = Files.newDirectoryStream(
					Paths.get(localProcessDir));
			for (Path file: stream) {
				currentFileName = file.toString().substring(
						file.toString().lastIndexOf(File.separator)+1);
				readXml(new FileInputStream(file.toString()),
						RecordType.HOLDINGS );
			}
			return;
		}
		
		if (localProcessFile != null) {
			currentFileName = localProcessFile.substring(
					localProcessFile.lastIndexOf(File.separator)+1);
			readXml(new FileInputStream(localProcessFile),
					RecordType.HOLDINGS );
			return;
		}

		// At this point we're looking for Dav sources
		if (mfhdSrcDir != null) {
			List<String> files = mfhdSrcDav.getFileUrlList(mfhdSrcDir);
			for ( String file : files) {
				currentFileName = file.substring(file.lastIndexOf('/')+1);
				readXml(mfhdSrcDav.getFileAsInputStream(file),
						RecordType.HOLDINGS );
			}
			return;
		}
		
		if (mfhdSrcFile != null) {
			currentFileName = mfhdSrcFile.substring(mfhdSrcFile.lastIndexOf('/')+1);
			readXml(mfhdSrcDav.getFileAsInputStream(mfhdSrcFile),
					RecordType.HOLDINGS );
			return;
		}
	}

	private void determineTargetBatches() throws IOException, XMLStreamException {
		String dirToProcess = null;
		String dirToProcessInto = null;
		
		if (tempDestDir != null)
			dirToProcessInto = tempDestDir;
		else if (destDir != null && isDestDav.equals(false))
			dirToProcessInto = destDir;
		else
			throw new IllegalArgumentException("Don't know where to put files!");

		Collection<Integer> bibids = new HashSet<Integer>();

		if (tempBibSrcDir != null)
			dirToProcess = tempBibSrcDir;
		else if (bibSrcDir != null && isBibSrcDav.equals(false))
			dirToProcess = bibSrcDir;
		if (dirToProcess != null) {
			System.out.println(dirToProcess);
			DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(dirToProcess));
			for (Path file: stream)
				bibids.addAll(collectBibidsFromXmlFile(file));
		} else {
			bibids.addAll(collectBibidsFromXmlFile(Paths.get(bibSrcFile)));
		}
		
		// Sort list of bib record IDs and determine ranges for batches of size groupsize.
		System.out.println(bibids.size() + " bibids in set.\n");
		Integer[] bibs = bibids.toArray(new Integer[ bibids.size() ]);
		bibids.clear();
		Arrays.sort( bibs );
		int batchCount = (bibs.length / groupsize) + 1;
		for (int i3 = 1; i3 <= batchCount; i3++) {
			Integer minBibid;
			if (i3*groupsize <= bibs.length)
				minBibid = bibs[(i3)*groupsize];
			else
				minBibid = bibs[bibs.length - 1];
			System.out.println(i3+": "+minBibid);
			BufferedOutputStream out =  new BufferedOutputStream(new GZIPOutputStream(
					new FileOutputStream(dirToProcessInto+"/"+
							destFilenamePrefix+"."+i3+".nt.gz", true)));
			outsById.put(minBibid, out);
			
		}

	}
	
	private Collection<Integer> collectBibidsFromXmlFile(Path file) throws XMLStreamException, IOException {
		Collection<Integer> bibids = new HashSet<Integer>();
		System.out.println(file.getFileName());
		XMLInputFactory input_factory = XMLInputFactory.newInstance();
		InputStream is = new FileInputStream(file.toString());
		XMLStreamReader r = input_factory.createXMLStreamReader(is);
		EVENT: while (r.hasNext()) {
			String event = getEventTypeString(r.next());
			if (event.equals("START_ELEMENT")) {
				if (r.getLocalName().equals("controlfield")) {
					for (int i1 = 0; i1 < r.getAttributeCount(); i1++)
						if (r.getAttributeLocalName(i1).equals("tag")) {
							if (r.getAttributeValue(i1).equals("001")) 
								bibids.add(Integer.valueOf(r.getElementText()));
							continue EVENT;
						}
				}
			}
		}
		is.close();
		return bibids;
	}

	private String downloadBibsToTempDir() throws Exception {
		Path tempLocalBibDir = Files.createTempDirectory("IL-xml2NT-");
		if (bibSrcDir != null) {
			List<String> bibSrcFiles = bibSrcDav.getFileUrlList(bibSrcDir);
			Iterator<String> i = bibSrcFiles.iterator();
			while (i.hasNext()) {
				String srcFile = i.next();
				String filename = srcFile.substring(srcFile.lastIndexOf('/') + 1);
				System.out.println(filename);
				bibSrcDav.getFile(srcFile, tempLocalBibDir + File.separator + filename);
				System.out.println(srcFile + ": " + tempLocalBibDir + File.separator + filename);
			}
		} else {
			bibSrcDav.getFile(bibSrcFile, tempLocalBibDir + File.separator + 
					bibSrcFile.substring( bibSrcFile.lastIndexOf('/') + 1 ));
		}
		return tempLocalBibDir.toString();
	}
	
	private void validateRun() {
		processingBibs = ((bibSrcDir != null) || (bibSrcFile != null));
		processingMfhds = ((mfhdSrcDir != null) || (mfhdSrcFile != null));
		if ( ! processingBibs && ! processingMfhds ) {
			System.out.println("At least one of bibs and mfhds must be configured for converstion.");
			throw new IllegalArgumentException("At least one of bibs and mfhds must be configured for converstion.");
		}
		Boolean destinationSupplied = ((destDir != null) || (destFile != null));
		if ( ! destinationSupplied ) {
			System.out.println("Either a destination directory or destination file must be configured.");
			throw new IllegalArgumentException("Either a destination directory or destination file must be configured.");
		}
		if ((bibSrcDir != null) && (bibSrcFile != null)) {
			System.out.println("To avoid ambiguity, a bib source directory and bib source file may not both be configured.");
			throw new IllegalArgumentException("To avoid ambiguity, a bib source directory and bib source file may not both be configured.");
		}
		if ((mfhdSrcDir != null) && (mfhdSrcFile != null)) {
			System.out.println("To avoid ambiguity, a mfhd source directory and mfhd source file may not both be configured.");
			throw new IllegalArgumentException("To avoid ambiguity, a mfhd source directory and mfhd source file may not both be configured.");
		}
		if ((destDir != null) && (destFile != null)) {
			System.out.println("To avoid ambiguity, a destination directory and destination file may not both be configured.");
			throw new IllegalArgumentException("To avoid ambiguity, a destination directory and destination file may not both be configured.");
		}
		if (destFilenamePrefix != null) {
			if (destFile != null) {
				System.out.println("When destination is a specific file, destination filename prefix may not be configured.");
				throw new IllegalArgumentException("When destination is a specific file, destination filename prefix may not be configured.");
			}
			if (mode.equals(Mode.NAME_AS_SOURCE)) {
				System.out.println("When processing in NAME_AS_SOURCE mode, destination filename prefix may not be configured.");
				throw new IllegalArgumentException("When processing in NAME_AS_SOURCE mode, destination filename prefix may not be configured.");
			}
		}
		if ( destDir != null && ! mode.equals(Mode.NAME_AS_SOURCE) && destFilenamePrefix == null) {
			System.out.println("When destination is a directory and not processing in NAME_AS_SOURCE mode, destination filename prefix must be configured.");
			throw new IllegalArgumentException("When destination is a directory and not processing in NAME_AS_SOURCE mode, destination filename prefix must be configured.");
		}
		if ( ! mode.equals(Mode.NAME_AS_SOURCE) && (destDir == null)) {
			System.out.println("When processing in RECORD_COUNT_BATCHES or ID_RANGE_BATCHES mode, a destination directory must be configured.");
			throw new IllegalArgumentException("When processing in RECORD_COUNT_BATCHES or ID_RANGE_BATCHES mode, a destination directory must be configured.");
		}
		if ((destFile != null) && ((bibSrcDir != null) || (mfhdSrcDir != null))) {
			System.out.println("When into a single destination file, source XML must be specified as files rather than directories.");
			throw new IllegalArgumentException("When into a single destination file, source XML must be specified as files rather than directories.");
		}
		if (destFile != null) {
			String dest = destFile.substring( destFile.lastIndexOf( isDestDav ? '/' : File.separatorChar) );
			if (processingBibs) {
				String bibFile = bibSrcFile.substring( bibSrcFile.lastIndexOf( isBibSrcDav ? '/' : File.separatorChar) );
				if ( ! bibFile.equals(dest)) {
					System.out.println("When processing bib and/or mfhd files into a single dest file, file names must match.");
					throw new IllegalArgumentException("When processing bib and/or mfhd files into a single dest file, file names must match.");
				}
			}
			if (processingBibs) {
				String mfhdFile = mfhdSrcFile.substring( mfhdSrcFile.lastIndexOf( isMfhdSrcDav ? '/' : File.separatorChar) );
				if ( ! mfhdFile.equals(dest)) {
					System.out.println("When processing bib and/or mfhd files into a single dest file, file names must match.");
					throw new IllegalArgumentException("When processing bib and/or mfhd files into a single dest file, file names must match.");
				}
			}
		}

	}
	
	
	private static Pattern shadowLinkPattern 
	   = Pattern.compile("https?://catalog.library.cornell.edu/cgi-bin/Pwebrecon.cgi\\?BBID=([0-9]+)&DB=local");
	private static Collection<String> shadowLinkedRecs = new HashSet<String>();
	/*
@Deprecated
	public static void marcXmlToNTriples(Collection<Integer> unsuppressedBibs,
			 							 Collection<Integer> unsuppressedMfhds,
			 							 DavService davService,
			 							 String bibSrcDir,
			 							 String mfhdSrcDir,
			 							 File target) throws Exception {

		BufferedOutputStream out =  new BufferedOutputStream(new GZIPOutputStream(
						new FileOutputStream(target, true)));
		
		RecordType type = RecordType.BIBLIOGRAPHIC;
		List<String> bibSrcFiles = davService.getFileUrlList(bibSrcDir);
		Iterator<String> i = bibSrcFiles.iterator();
		while (i.hasNext()) {
			String srcFile = i.next();
			InputStream xmlstream = davService.getFileAsInputStream(srcFile);
			XMLInputFactory input_factory = XMLInputFactory.newInstance();
			XMLStreamReader r  = 
					input_factory.createXMLStreamReader(xmlstream);
			processRecords(r,type,unsuppressedBibs,out);
			xmlstream.close();
		}
		
		type = RecordType.HOLDINGS;
		List<String> mfhdSrcFiles = davService.getFileUrlList(mfhdSrcDir);
		i = mfhdSrcFiles.iterator();
		while (i.hasNext()) {
			String srcFile = i.next();
			InputStream xmlstream = davService.getFileAsInputStream(srcFile);
			XMLInputFactory input_factory = XMLInputFactory.newInstance();
			XMLStreamReader r  = 
					input_factory.createXMLStreamReader(xmlstream);
			processRecords(r,type,unsuppressedMfhds,out);
			xmlstream.close();
		}
		out.flush();
		out.close();

	}
*/
	/**
	 * Organize bibs in bibSrcDir and mfhds in mfhdSrcDir into nt.gz files in targetDir. The records
	 * will be sorted by bibiographic id and grouped into nt.gz files of groupsize records. Records
	 * not on the list of unsuppressedBibs or unsuppressedMfhds, as appropriate, will be discarded.
	 * 
	 * @param unsuppressedBibs
	 * @param unsuppressedMfhds
	 * @param davService
	 * @param bibSrcDir
	 * @param mfhdSrcDir
	 * @param targetDir
	 * @throws Exception
	 */
/*
@Deprecated
	public static void marcXmlToNTriples(Collection<Integer> unsuppressedBibs,
			 Collection<Integer> unsuppressedMfhds,
			 DavService davService,
			 String bibSrcDir,
			 String mfhdSrcDir,
			 Path targetDir) throws Exception {

		// Download all bib xml
		Path tempLocalBibDir = Files.createTempDirectory("IL-updatesBibs");
		List<String> bibSrcFiles = davService.getFileUrlList(bibSrcDir);
		Iterator<String> i = bibSrcFiles.iterator();
		while (i.hasNext()) {
			String srcFile = i.next();
			String filename = srcFile.substring(srcFile.lastIndexOf('/') + 1);
			System.out.println(filename);
			davService.getFile(srcFile, tempLocalBibDir+"/"+filename);
			System.out.println(srcFile+": "+tempLocalBibDir+"/"+filename);
		}
		
		// Preprocess bib xml files for a list of bib record IDs.
		System.out.println(tempLocalBibDir);
		Collection<Integer> bibids = new HashSet<Integer>();
		DirectoryStream<Path> stream = Files.newDirectoryStream(tempLocalBibDir);
		for (Path file: stream) {
			System.out.println(file.getFileName());
			XMLInputFactory input_factory = XMLInputFactory.newInstance();
			InputStream is = new FileInputStream(file.toString());
			XMLStreamReader r = input_factory.createXMLStreamReader(is);
			EVENT: while (r.hasNext()) {
				String event = getEventTypeString(r.next());
				if (event.equals("START_ELEMENT")) {
					if (r.getLocalName().equals("controlfield")) {
						for (int i1 = 0; i1 < r.getAttributeCount(); i1++)
							if (r.getAttributeLocalName(i1).equals("tag")) {
								if (r.getAttributeValue(i1).equals("001")) 
									bibids.add(Integer.valueOf(r.getElementText()));
								continue EVENT;
							}
					}
				}
			}
			is.close();
		}
		
		// Sort list of bib record IDs and determine ranges for batches of size groupsize.
		System.out.println(bibids.size() + " bibids in set.\n");
		Integer[] bibs = bibids.toArray(new Integer[ bibids.size() ]);
		bibids.clear();
		Arrays.sort( bibs );
		int batchCount = (bibs.length / groupsize) + 1;
		Map<Integer,BufferedOutputStream> outs = new HashMap<Integer,BufferedOutputStream>();
		for (int i3 = 1; i3 <= batchCount; i3++) {
			Integer minBibid;
			if (i3*groupsize <= bibs.length)
				minBibid = bibs[(i3)*groupsize];
			else
				minBibid = bibs[bibs.length - 1];
			System.out.println(i3+": "+minBibid);
			BufferedOutputStream out =  new BufferedOutputStream(new GZIPOutputStream(
					new FileOutputStream(targetDir+"/"+i3+".nt.gz", true)));
			outs.put(minBibid, out);
			
		}
		
		// Process bibs into determined batches, deleting local copies when done.
		RecordType type = RecordType.BIBLIOGRAPHIC;
		stream = Files.newDirectoryStream(tempLocalBibDir);
		for (Path file: stream) {
			InputStream is = new FileInputStream(file.toString());
			XMLInputFactory input_factory = XMLInputFactory.newInstance();
			XMLStreamReader r  = 
					input_factory.createXMLStreamReader(is);
			processRecords(r,type,unsuppressedBibs,outs);
			is.close();
			Files.delete(file);
		}
		Files.delete(tempLocalBibDir);
		
		// Process holdings records directly from webDav. Since we only need to look
		// at them once, there's no need for local copies.
		type = RecordType.HOLDINGS;
		List<String> mfhdSrcFiles = davService.getFileUrlList(mfhdSrcDir);
		i = mfhdSrcFiles.iterator();
		while (i.hasNext()) {
			String srcFile = i.next();
			InputStream xmlstream = davService.getFileAsInputStream(srcFile);
			XMLInputFactory input_factory = XMLInputFactory.newInstance();
			XMLStreamReader r  = 
					input_factory.createXMLStreamReader(xmlstream);
			processRecords(r,type,unsuppressedMfhds,outs);
			xmlstream.close();
		}
		
		// Close all of the output handles.
		Iterator<Integer> outIter = outs.keySet().iterator();
		while (outIter.hasNext()) {
			Integer minBibid = outIter.next();
			outs.get(minBibid).flush();
			outs.get(minBibid).close();
		}

	}

@Deprecated	
	private static void processRecords (XMLStreamReader r,
										RecordType type,
										Collection<Integer> unsuppressedList,
										Map<Integer,BufferedOutputStream> outs) throws Exception {
		while (r.hasNext()) {
			String event = getEventTypeString(r.next());
			if (event.equals("START_ELEMENT"))
				if (r.getLocalName().equals("record")) {
					MarcRecord rec = processRecord(r);
					rec.type = type;
					
					Integer id = Integer.valueOf(rec.id);
					if (unsuppressedList.contains(id)) {
						// Remove id from list to prevent processing duplicates, and to
						// create list of not-found bibs. This list is more important for
						// full updates than incrementals.
						unsuppressedList.remove(id);
					} else {
//						System.out.println("Record not on unsuppressed list, " + id + " - skipping.");
						continue;
					}
					
//					tabulateFieldData(rec);
					identifyShadowRecordTargets(rec);
//					extractData(rec);
//					mapNonRomanFieldsToRomanizedFields(rec);
//					if (rec.type == RecordType.BIBLIOGRAPHIC) 
//						attemptToConfirmDateValues(rec);
					String ntriples = generateNTriples( rec, type );

					Integer bibid = 0;
					if (type.equals(RecordType.BIBLIOGRAPHIC)) {
						bibid = Integer.valueOf(rec.id);
					} else if (type.equals(RecordType.HOLDINGS)) {
						bibid = Integer.valueOf(rec.bib_id);
					}
					Integer outputBatch = 100_000_000;
					Iterator<Integer> outIter = outs.keySet().iterator();
					while (outIter.hasNext()) {
						Integer batch = outIter.next();
						if ((bibid <= batch) && (outputBatch > batch))
							outputBatch = batch;
					}
					if (outputBatch == 100_000_000) {
						System.out.println("Failed to identify output batch for bib "+bibid
								+". Not writing "+type+" record to N-Triples.");
						continue;
					}
					outs.get(outputBatch).write( ntriples.getBytes() );
				}
		}
		
	}

@Deprecated
	private static void processRecords (XMLStreamReader r,
										RecordType type,
										Collection<Integer> unsuppressedList,
										BufferedOutputStream out) throws Exception {
		while (r.hasNext()) {
			String event = getEventTypeString(r.next());
			if (event.equals("START_ELEMENT"))
				if (r.getLocalName().equals("record")) {
					MarcRecord rec = processRecord(r);
					rec.type = type;
					
					Integer id = Integer.valueOf(rec.id);
					if (unsuppressedList.contains(id)) {
						// Remove id from list to prevent processing duplicates, and to
						// create list of not-found bibs. This list is more important for
						// full updates than incrementals.
						unsuppressedList.remove(id);
					} else {
//						System.out.println("Record not on unsuppressed list, " + id + " - skipping.");
						continue;
					}
					
//					tabulateFieldData(rec);
					identifyShadowRecordTargets(rec);
//					extractData(rec);
//					mapNonRomanFieldsToRomanizedFields(rec);
//					if (rec.type == RecordType.BIBLIOGRAPHIC) 
//						attemptToConfirmDateValues(rec);
					String ntriples = generateNTriples( rec, type );
					out.write( ntriples.getBytes() );
				}
		}
		
	}
*/
	/*
	
@Deprecated
	public static void marcXmlToNTriples(File xmlfile, File targetfile) throws Exception {
		RecordType type ;
		if (xmlfile.getName().startsWith("mfhd"))
			type = RecordType.HOLDINGS;
		else if (xmlfile.getName().startsWith("auth"))
			type = RecordType.AUTHORITY;
		else if (xmlfile.getName().startsWith("bib"))
			type = RecordType.BIBLIOGRAPHIC;
		else { 
			System.out.println("Not processing file. Record type unidentified from filename prefix.");
			return;
		}		
		marcXmlToNTriples( xmlfile, targetfile, type );
	}
	
@Deprecated
	public static void marcXmlToNTriples(File xmlfile, File target, RecordType type) throws Exception {
		FileInputStream xmlstream = new FileInputStream( xmlfile );
		XMLInputFactory input_factory = XMLInputFactory.newInstance();
		XMLStreamReader r  = 
				input_factory.createXMLStreamReader(xmlfile.getPath(), xmlstream);
		BufferedOutputStream out = null;
		if (! target.isDirectory()) {
			out =  new BufferedOutputStream(new GZIPOutputStream(
						new FileOutputStream(target, true)));
		}
				
		String curfile = "";
		while (r.hasNext()) {
			String event = getEventTypeString(r.next());
			if (event.equals("START_ELEMENT"))
				if (r.getLocalName().equals("record")) {
					MarcRecord rec = processRecord(r);
					rec.type = type;
					
					Integer id = Integer.valueOf(rec.id);
					if (unsuppressedRecs.contains(id)) {
						unsuppressedRecs.remove(id);
					} else {
//						System.out.println("Record not on suppressed or unsuppressed lists, "
//								+ id + " - skipping.");
						continue;
					}
					
					// Protecting against duplicate records in a single batch.
					// Note: This will be a bug if processing multiple record types
					// in a single instance of MarcXmlToNTriples.
					if (foundRecs.contains(Integer.valueOf(rec.id))) continue;
					else foundRecs.add(Integer.valueOf(rec.id));

		//			tabulateFieldData(rec);
					identifyShadowRecordTargets(rec);
					extractData(rec);
		//			mapNonRomanFieldsToRomanizedFields(rec);
					surveyForCJKValues(rec);
					if (rec.type == RecordType.BIBLIOGRAPHIC) 
						attemptToConfirmDateValues(rec);
					String ntriples = generateNTriples( rec, type );
					String file = type.toString().toLowerCase() + '.' +
							(Integer.valueOf(rec.id) / groupsize) + 
							".nt.gz";
					if (rec.type == RecordType.HOLDINGS) {
//						file = type.toString().toLowerCase() + '.' +
						file = "bibliographic" + '.' +
								(Integer.valueOf(rec.bib_id) / groupsize) + 
								".nt.gz";
					}
					if (target.isDirectory()) {
						if (! file.equals(curfile)) {
							if (! curfile.equals("")) {
								out.flush();
								out.close();
							}
							System.out.println("    =>  "+target + "/" +file);
							out =  new BufferedOutputStream(new GZIPOutputStream(
									new FileOutputStream(target + "/" + file, true)));
							curfile = file;
						}
					}
					out.write( ntriples.getBytes() );
				}
		}
		xmlstream.close();
		if (curfile.isEmpty()) {
			System.out.println("    =>  FILE CONTAINS NO UNSUPPRESSED RECORDS.");
		} else {
			out.flush();
			out.close();
		}
	}
*/
	public static void surveyForCJKValues( MarcRecord rec ) throws IOException {
		if (logout == null) {
			FileWriter logstream = new FileWriter(logfile);
			logout = new BufferedWriter( logstream );
		}

		Map<Integer,DataField> datafields = rec.data_fields;
		for (DataField f : datafields.values() ) {
			String text = f.concateSubfieldsOtherThan6();
			if (f.tag.equals("880")) {
				MarcRecord.Script script = f.getScript();
				if (script.equals(MarcRecord.Script.CJK)) {
					if (! hasCJK(text))
						logout.write("CJKError: ("+rec.type.toString()+":" + rec.id + 
								") 880 field labeled CJK but doesn't appear to be: "+f.toString()+"\n");
				} else {
					if (isCJK(text))
						logout.write("CJKError: ("+rec.type.toString()+":" + rec.id + 
								") 880 field appears to be CJK but isn't labeled that way: "+f.toString()+"\n");
 				}	
			} else {
				if (isCJK(text))
					logout.write("CJKError: ("+rec.type.toString()+":" + rec.id + 
							") non-880 field appears to contain CJK text: "+f.toString()+"\n");
			}
		}
	}
	
	/**
	 * @param args
	 *//*
@Deprecated
	public static void main(String[] args) {
		
		String unsuppressedFile = "/users/fbw4/voyager-harvest/data/fulldump/unsuppressed.txt";

		Path path = Paths.get(unsuppressedFile);
		try {
			Scanner scanner = new Scanner(path,StandardCharsets.UTF_8.name());
			while (scanner.hasNextLine()) {
				String id = scanner.nextLine();
				unsuppressedRecs.add(Integer.valueOf(id));
			}
			scanner.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		System.out.println("Expecting " + unsuppressedRecs.size() + " unsuppressed records.");
		String destdir = "/users/fbw4/voyager-harvest/data/clean";
		File file = new File( "/users/fbw4/voyager-harvest/data/fulldump" );
		File[] files = file.listFiles();
		for (File f: files) {
			System.out.println(f);
			try {
				marcXmlToNTriples( f, new File(destdir) );
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		String[] tags = fieldStatsByTag.keySet().toArray(new String[ fieldStatsByTag.keySet().size() ]);
		Arrays.sort( tags );
		for( String tag: tags) {
			System.out.println("--------------------------------");
			System.out.println(fieldStatsByTag.get(tag).toString());
		}
		if (unsuppressedRecs.size() > 0) {
			System.out.println("Unsuppressed records expected but not found:");
			for (Integer id: unsuppressedRecs) {
				System.out.print(id+", ");
			}
			System.out.println();
		}
		try {
			if (shadowLinkedRecs.size() > 0) {
				BufferedOutputStream shadowOut =  new BufferedOutputStream(new GZIPOutputStream(
							new FileOutputStream(destdir+"/shadows.nt.gz", true)));
				String relation = "<http://da-rdf.library.cornell.edu/integrationLayer/0.1/boost>";
				String target = "shadowLink";
				for (String id: shadowLinkedRecs) {
					String bib = "<http://da-rdf.library.cornell.edu/individual/b"+id+">";
					String triple = bib + " " + relation + " \"" + target + "\".\n";
					shadowOut.write(triple.getBytes());
				}
				shadowOut.flush();
				shadowOut.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
@Deprecated
	private static void identifyShadowRecordTargets(MarcRecord rec) {
		for (Integer fid: rec.data_fields.keySet()) {
			DataField f = rec.data_fields.get(fid);
			if (f.tag.equals("856")) {
				for (Integer sfid: f.subfields.keySet()) {
					Subfield sf = f.subfields.get(sfid);
					if (sf.code.equals('u')) {
						Matcher m = shadowLinkPattern.matcher(sf.value);
						if (m.matches()) {
							String linked_rec = m.group(1);
							System.out.println("Shadow Record links to "+linked_rec+
									" ("+sf.value+")");
							shadowLinkedRecs.add(linked_rec);
						}
					}
				}
			}
		}
	}
	*/
	
@Deprecated
	private static void extractData( MarcRecord rec ) throws Exception {

		Integer rec_id = Integer.valueOf( rec.control_fields.get(1).value );
	//	Pattern entity_p = Pattern.compile(".*&#?\\w+;.*");
		
		if ((extractout == null))  {
			FileWriter logstream = new FileWriter(extractfile,true);
			extractout = new BufferedWriter( logstream );
		}
		
		for (Integer fid: rec.data_fields.keySet()) {
			DataField f = rec.data_fields.get(fid);
			/*
			if (f.tag.equals("700")) {
				Iterator<Subfield> i = f.subfields.values().iterator();
				while (i.hasNext()) {
					Subfield sf = i.next();
					if (sf.code.equals('4')) {
						extractout.write(rec_id + "\t" 
			                     + f.toString() + "\n");
					}
				}
			}
			if (f.tag.equals("010")) {
				extractout.write(rec_id + "\t" + f.toString() + "\n");
			}
			if (f.tag.equals("020")) {
				extractout.write(rec_id + "\t" + f.toString() + "\n");
			}
			if (f.tag.equals("022")) {
				extractout.write(rec_id + "\t" + f.toString() + "\n");
			}
			if (f.tag.equals("035")) {
				extractout.write(rec_id + "\t" + f.toString() + "\n");
			}
			if (f.tag.equals("050")) {
				extractout.write(rec_id + "\t" + f.toString() + "\n");
			}
			*/
			if (f.tag.equals("852"))
				extractout.write(rec_id + "\t" + f.toString() + "\n");
			/*		if (f.tag.equals("856")) {
				extractout.write(rec_id + "\t" 
			                     + f.ind1 + "\t"
			                     + f.ind2 + "\t"
			                     + f.toString() + "\n");
			}
			if (f.tag.equals("948")) {
				Iterator<Subfield> i = f.subfields.values().iterator();
				while (i.hasNext()) {
					Subfield sf = i.next();
					if (sf.code.equals('f') && ! sf.value.equals("?")) {
						extractout.write(rec_id + "\t" 
			                     + f.ind1 + "\t"
			                     + f.ind2 + "\t"
			                     + "948" + sf.toString() + "\n");
					}					
				}
			}
			if (f.tag.equals("245")) {
				Iterator<Subfield> i = f.subfields.values().iterator();
				while (i.hasNext()) {
					Subfield sf = i.next();
					if (sf.code.equals('h')) {
						extractout.write(rec_id + "\t" 
			                     + f.ind1 + "\t"
			                     + f.ind2 + "\t"
			                     + "245" + sf.toString() + "\n");
					} else if (sf.code.equals('k')) {
						extractout.write(rec_id + "\t" 
			                     + f.ind1 + "\t"
			                     + f.ind2 + "\t"
			                     + f.toString() + "\n");						
					}
				}
			} */
/*			// entity in any datafield value
			Iterator<Subfield> i = f.subfields.values().iterator();
			while (i.hasNext()) {
				Subfield sf = i.next();
				Matcher m = entity_p.matcher(sf.value);
				if (m.matches()) {
					extractout.write(rec_id + "\t" 
		                     + f.toString() + "\n");
					break;
				}
			} 
			
			// Genre subfields
			if (f.tag.startsWith("6")) {
				Iterator<Subfield> i = f.subfields.values().iterator();
				while (i.hasNext()) {
					Subfield sf = i.next();
					if (sf.code.equals('v') || 
							(f.tag.equals("655") && sf.code.equals('a'))) {
						extractout.write(rec_id + "\t" 
			                     + f.ind1 + "\t"
			                     + f.ind2 + "\t"
			                     + f.tag + sf.toString() + "\n");						
					}
				}
			}
			*/
		}
	
		if (0 == (rec_id % 1000)) {
			extractout.flush();
			extractout.close();
			extractout = null;
		}
	}

	private String buildGenFreqReport() {
		String[] tags = fieldStatsByTag.keySet().toArray(new String[ fieldStatsByTag.keySet().size() ]);
		Arrays.sort( tags );
		StringBuilder sb = new StringBuilder();
		Boolean first = true;
		for( String tag: tags) {
			if (first) first = false;
			else sb.append("-------------------------------\n");
			sb.append(fieldStatsByTag.get(tag).toString());
		}
		fieldStatsByTag.clear();
		return sb.toString();
	}
	
	private void tabulateFieldData( MarcRecord rec ) throws Exception {
		
		Map<String,Integer> fieldtagcounts = new HashMap<String,Integer>();
		Map<String,HashMap<Character,Integer>> codeCounts = 
				new HashMap<String,HashMap<Character,Integer>>();
		Integer rec_id = Integer.valueOf( rec.control_fields.get(1).value );
		
		if (logout == null) {
			FileWriter logstream = new FileWriter(logfile);
			logout = new BufferedWriter( logstream );
		}
		
		
		for (Integer fid: rec.control_fields.keySet()) {
			ControlField f = rec.control_fields.get(fid);
			if (fieldtagcounts.containsKey(f.tag)) {
				fieldtagcounts.put(f.tag, fieldtagcounts.get(f.tag)+1);
			} else {
				fieldtagcounts.put(f.tag, 1);
			}
		}
		for (Integer fid: rec.data_fields.keySet()) {
			DataField f = rec.data_fields.get(fid);
			if (fieldtagcounts.containsKey(f.tag)) {
				fieldtagcounts.put(f.tag, fieldtagcounts.get(f.tag)+1);
			} else {
				fieldtagcounts.put(f.tag, 1);
			}			
			if (! fieldStatsByTag.containsKey(f.tag)) {
				FieldStats fs = new FieldStats();
				fs.tag = f.tag;
				fieldStatsByTag.put(f.tag, fs);
			}
			FieldStats fs = fieldStatsByTag.get(f.tag);
			if (fs.countBy1st.containsKey(f.ind1)) {
				fs.countBy1st.put(f.ind1, fs.countBy1st.get(f.ind1)+ 1);
			} else {
				fs.countBy1st.put(f.ind1, 1);
				fs.exampleBy1st.put(f.ind1,rec_id);
			}
			if (fs.countBy2nd.containsKey(f.ind2)) {
				fs.countBy2nd.put(f.ind2, fs.countBy2nd.get(f.ind2)+ 1);
			} else {
				fs.countBy2nd.put(f.ind2, 1);
				fs.exampleBy2nd.put(f.ind2,rec_id);
			}
			String indpair = f.ind1.toString() + f.ind2.toString();
			if (fs.countByBoth.containsKey(indpair)) {
				fs.countByBoth.put(indpair, fs.countByBoth.get(indpair)+ 1);
			} else {
				fs.countByBoth.put(indpair, 1);
				fs.exampleByBoth.put(indpair,rec_id);
			}
			Integer[] subfields = f.subfields.keySet().toArray(new Integer[ f.subfields.keySet().size() ]);
			Arrays.sort( subfields );
			StringBuilder sb = new StringBuilder();
			for (Integer sfid: subfields) {
				Subfield sf = f.subfields.get(sfid);
				sb.append(sf.code);
				if (codeCounts.containsKey(f.tag)) {
					HashMap<Character,Integer> tagCounts = codeCounts.get(f.tag);
					if (tagCounts.containsKey(sf.code)) {
						tagCounts.put(sf.code, tagCounts.get(sf.code)+1);
					} else {
						tagCounts.put(sf.code,1);
					}
					codeCounts.put(f.tag, tagCounts);
				} else {
					HashMap<Character,Integer> tagCounts = new HashMap<Character,Integer>();
					tagCounts.put(sf.code, 1);
					codeCounts.put(f.tag, tagCounts);
				}
				if (f.tag.equals("245") && sf.code.equals('a')) {
					if (sf.value.length() <= 1)
						if (reports.contains(Report.QC_245))
						logout.write("Error: ("+rec.type.toString()+":" + rec_id + 
							") 245 subfield a has length of "+ sf.value.length()+ ": "+ f.toString() + "\n");
					else if (sf.value.trim().length() < 1)
						if (reports.contains(Report.QC_245))
						logout.write("Error: ("+rec.type.toString()+":" + rec_id + 
							") 245 subfield a contains only whitespace: "+ f.toString() + "\n");
					
				}
				if (! (Character.isLowerCase(sf.code) || Character.isDigit(sf.code))) {
					if (reports.contains(Report.QC_SUBFIELD_CODES))
					logout.write("Error: ("+rec.type.toString()+":" + rec_id + 
							") Field has subfield code \""+sf.code+"\" which is neither lower case nor a digit: "+ f.toString() +  "\n");
				}
			}
			String sfpattern = sb.toString();
			if (fs.countBySubfieldPattern.containsKey(sfpattern)) {
				fs.countBySubfieldPattern.put(sfpattern, fs.countBySubfieldPattern.get(sfpattern)+ 1);
			} else {
				fs.countBySubfieldPattern.put(sfpattern, 1);
				fs.exampleBySubfieldPattern.put(sfpattern,rec_id);
			}
			if (f.tag.equals("245") && ! sfpattern.contains("a")) {
				no245a.add(rec_id);
				if (reports.contains(Report.QC_245))
				logout.write("Error: ("+rec.type.toString()+":" + rec_id + 
						") 245 field has no subfield a: "+ f.toString() +  "\n");
			}
			
			fieldStatsByTag.put(f.tag, fs);
		}
		for( String tag: fieldtagcounts.keySet()) {
			Integer count = fieldtagcounts.get(tag);
			if (! fieldStatsByTag.containsKey(tag)) {
				FieldStats fs = new FieldStats();
				fs.tag = tag;
				fieldStatsByTag.put(tag, fs);
			}
			FieldStats fs = fieldStatsByTag.get(tag);
			if (fs.countByCount.containsKey(count)) {
				fs.countByCount.put(count, fs.countByCount.get(count)+1);
			} else {
				fs.countByCount.put(count, 1);
				fs.exampleByCount.put(count, rec_id);
			}
			fs.recordCount++;
			fs.instanceCount += count;
			fieldStatsByTag.put(tag, fs);
		}
		for (String tag: codeCounts.keySet()) {
			HashMap<Character,Integer> tagCounts = codeCounts.get(tag);
			FieldStats fs = fieldStatsByTag.get(tag);
			for (Character code: tagCounts.keySet()) {
				if (! fs.subfieldStatsByCode.containsKey(code)) {
					SubfieldStats sfs = new SubfieldStats();
					sfs.code = code;
					fs.subfieldStatsByCode.put(code, sfs);
				}
				SubfieldStats sfs = fs.subfieldStatsByCode.get(code);
				sfs.recordCount++;
				sfs.instanceCount += tagCounts.get(code);
				fs.subfieldStatsByCode.put(code, sfs);
			}
			fieldStatsByTag.put(tag, fs);
		}
		recordCount++;
	}

	private String generateNTriples ( MarcRecord rec, RecordType type ) {
		StringBuilder sb = new StringBuilder();
		String id = rec.control_fields.get(1).value;
		rec.id = id;
		String uri_host = "http://da-rdf.library.cornell.edu/individual/";
		if (uriPrefix != null) 
			uri_host = uriPrefix;
		String id_pref;
		String record_type_uri;
		if (type == RecordType.BIBLIOGRAPHIC) {
			id_pref = "b";
			record_type_uri = "<http://marcrdf.library.cornell.edu/canonical/0.1/BibliographicRecord>";
		} else if (type == RecordType.HOLDINGS) {
			id_pref = "h";
			record_type_uri = "<http://marcrdf.library.cornell.edu/canonical/0.1/HoldingsRecord>";
		} else { //if (type == RecordType.AUTHORITY) {
			id_pref = "a";
			record_type_uri = "<http://marcrdf.library.cornell.edu/canonical/0.1/AuthorityRecord>";
		}
		String record_uri = "<"+uri_host+id_pref+id+">";
		sb.append(record_uri + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " + record_type_uri +" .\n");
		sb.append(record_uri + " <http://www.w3.org/2000/01/rdf-schema#label> \""+id+"\".\n");
		if (type.equals(RecordType.BIBLIOGRAPHIC) && isUnsuppressedBibListFlagged)
			sb.append(record_uri + " <http://marcrdf.library.cornell.edu/canonical/0.1/status> \""
					+((unsuppressedBibs.contains(id))?"unsuppressed":"suppressed")+  "\".\n");
		if (type.equals(RecordType.HOLDINGS) && isUnsuppressedMfhdListFlagged)
			sb.append(record_uri + " <http://marcrdf.library.cornell.edu/canonical/0.1/status> \""
					+((unsuppressedMfhds.contains(id))?"unsuppressed":"suppressed")+  "\".\n");
		sb.append(record_uri + " <http://marcrdf.library.cornell.edu/canonical/0.1/leader> \""+rec.leader+"\".\n");
		int fid = 0;
		while( rec.control_fields.containsKey(fid+1) ) {
			ControlField f = rec.control_fields.get(++fid);
			String field_uri = "<"+uri_host+id_pref+id+"_"+fid+">";
			sb.append(record_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/hasField> "+field_uri+".\n");
			sb.append(record_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/hasField"+f.tag+"> "+field_uri+".\n");
			sb.append(field_uri+" <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://marcrdf.library.cornell.edu/canonical/0.1/ControlField> .\n");
			sb.append(field_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/tag> \""+escapeForNTriples(f.tag)+"\".\n");
			sb.append(field_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/value> \""+escapeForNTriples(f.value)+"\".\n");
			if ((f.tag.contentEquals("004")) && (type == RecordType.HOLDINGS)) {
				rec.bib_id = f.value;
				sb.append(record_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/hasBibliographicRecord> <"+uri_host+"b"+escapeForNTriples(f.value)+">.\n");
			}
		}
		while( rec.data_fields.containsKey(fid+1) ) {
			DataField f = rec.data_fields.get(++fid);
			String field_uri = "<"+uri_host+id_pref+id+"_"+fid+">";
			sb.append(record_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/hasField> "+field_uri+".\n");
			sb.append(record_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/hasField"+f.tag+"> "+field_uri+".\n");
			if (f.alttag != null)
				sb.append(record_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/hasField"+f.alttag+"> "+field_uri+".\n");
			sb.append(field_uri+" <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://marcrdf.library.cornell.edu/canonical/0.1/DataField> .\n");
			sb.append(field_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/tag> \""+escapeForNTriples(f.tag)+"\".\n");
			sb.append(field_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/ind1> \""+escapeForNTriples(f.ind1.toString())+"\".\n");
			sb.append(field_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/ind2> \""+escapeForNTriples(f.ind2.toString())+"\".\n");

			int sfid = 0;
			while( f.subfields.containsKey(sfid+1) ) {
				Subfield sf = f.subfields.get(++sfid);
				String subfield_uri = "<"+uri_host+id_pref+id+"_"+fid+"_"+sfid+">";
				sb.append(field_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/hasSubfield> "+subfield_uri+".\n");
				sb.append(subfield_uri+" <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://marcrdf.library.cornell.edu/canonical/0.1/Subfield> .\n");
				sb.append(subfield_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/code> \""+escapeForNTriples(sf.code.toString())+"\".\n");
				sb.append(subfield_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/value> \""+escapeForNTriples( sf.value )+"\".\n");
			}

		}

		return sb.toString();
	}
	
@Deprecated
	private static void attemptToConfirmDateValues( MarcRecord rec ) throws Exception {
		
		Collection<String> humanDates = new HashSet<String>();
		Collection<String> machineDates = new HashSet<String>();
		Pattern p = Pattern.compile("^[0-9]{4}$");
		Boolean found008 = false;
		String rec_id = rec.control_fields.get(1).value;
		int current_year = Calendar.getInstance().get(Calendar.YEAR);

		if (logout == null) {
			FileWriter logstream = new FileWriter(logfile);
			logout = new BufferedWriter( logstream );
		}
		
//		logout.write("------------------------------\n");
		
		for (int id: rec.control_fields.keySet()) {
			ControlField f = rec.control_fields.get(id);
			if (f.tag.equals("008")) {
				if (found008) {
					logout.write("Error: ("+rec.type.toString()+":" + rec_id + 
							") More than one 008 found in record.\n");
				}
				found008 = true;
//				logout.write("008: "+f.value);
				String date1 = f.value.substring(7, 11);
				String date2 = f.value.substring(11, 15);
				Matcher m = p.matcher(date1);
				if (m.matches() && ! date1.equals("9999"))
					machineDates.add(date1);
				m = p.matcher(date2);
				if (m.matches() && ! date2.equals("9999"))
					machineDates.add(date2);
			}
		}
		if (!found008) {
			logout.write("Error: ("+rec.type.toString()+":" + rec_id + 
					") No 008 found in record.\n");
		}
		
		for (int id: rec.data_fields.keySet()) {
			DataField f = rec.data_fields.get(id);
			if (f.tag.equals("260") || f.tag.equals("264")) {
//				logout.write(f.toString());
				for ( int sf_id: f.subfields.keySet() ) {
					Subfield sf = f.subfields.get(sf_id);
					if (sf.code.equals('c')) {
						humanDates.add(sf.value);
					}
				}
			}
			
		}
		if (humanDates.isEmpty()) {
//			logout.write("Error: ("+rec.type.toString()+":" + rec_id + 
//					") Record has no 260 or 264 subfield c.\n");			
		}
		if (machineDates.isEmpty()) return;
		for ( String date: machineDates) {
			if (Integer.valueOf(date) > current_year + 1) {
				logout.write("Error: ("+rec.type.toString()+":" + rec_id + 
						") Date of "+date+"in 008 field is in the future.\n");			
			} else {
				Boolean found = false;
				for (String hDate: humanDates) {
					if (hDate.contains(date)) {
						found = true;
						break;
					}
				}
				if (! found) {
					// This appears to be an extremely weak indicator of error.
//					logout.write("Error: ("+rec.type.toString()+":" + rec_id + 
//							") Date in 008, "+date+", cannot be found in 260 or 264.\n");			
				}
			}
		}
	}

	private void mapNonRomanFieldsToRomanizedFields( MarcRecord rec ) throws Exception {
		Map<Integer,Integer> linkedeighteighties = new HashMap<Integer,Integer>();
//		Map<Integer,String> unlinkedeighteighties = new HashMap<Integer,String>();
		Map<Integer,Integer> others = new HashMap<Integer,Integer>();
		String rec_id = rec.control_fields.get(1).value;
		Pattern p = Pattern.compile("^[0-9]{3}.[0-9]{2}.*");
		
		if (logout == null) {
			FileWriter logstream = new FileWriter(logfile);
			logout = new BufferedWriter( logstream );
		}

		for ( int id: rec.data_fields.keySet() ) {
			DataField f = rec.data_fields.get(id);
			for ( int sf_id: f.subfields.keySet() ) {
				Subfield sf = f.subfields.get(sf_id);
				if (sf.code.equals('6')) {
					Matcher m = p.matcher(sf.value);
					if (m.matches()) {
						int n = Integer.valueOf(sf.value.substring(4, 6));
						if (f.tag.equals("880")) {
							f.alttag = sf.value.substring(0, 3);
							if (n == 0) {
//								unlinkedeighteighties.put(id, sf.value.substring(0, 3));
							} else {
								if (linkedeighteighties.containsKey(n)) {
									if (reports.contains(Report.QC_880))
										logout.write("Error: ("+rec.type.toString()+":" + rec_id + ") More than one 880 with the same link index.\n");
								}
								linkedeighteighties.put(n, id);
							}
						} else {
							if (others.containsKey(n)) {
								if (reports.contains(Report.QC_880))
								logout.write("Error: ("+rec.type.toString()+":" + rec_id + ") More than one field linking to 880s with the same link index.\n");
							}
							others.put(n, id);
						}
					} else {
						if (reports.contains(Report.QC_880))
						logout.write("Error: ("+rec.type.toString()+":" + rec_id +") "+
								f.tag+" field has ‡6 with unexpected format: \""+sf.value+"\".\n");
					}
				}
			}
		}

//		for( int fid: unlinkedeighteighties.keySet() ) {
//			rec.data_fields.get(fid).alttag = unlinkedeighteighties.get(fid);
//		}
		for( int link_id: others.keySet() ) {
			if (linkedeighteighties.containsKey(link_id)) {
				// LINK FOUND
//				rec.data_fields.get(linkedeighteighties.get(link_id)).alttag = rec.data_fields.get(others.get(link_id)).tag;
			} else {
				if (reports.contains(Report.QC_880))
				logout.write("Error: ("+rec.type.toString()+":" + rec_id + ") "+
						rec.data_fields.get(others.get(link_id)).tag+
						" field linking to non-existant 880.\n");
			}
		}
		for ( int link_id: linkedeighteighties.keySet() )
			if ( ! others.containsKey(link_id))
				if (reports.contains(Report.QC_880))
					logout.write("Error: ("+rec.type.toString()+":" + rec_id + ") 880 field linking to non-existant main field.\n");
			logout.flush();
	}
		
	private MarcRecord processRecord( XMLStreamReader r ) throws Exception {
		
		MarcRecord rec = new MarcRecord();
		int id = 0;
		while (r.hasNext()) {
			String event = getEventTypeString(r.next());
			if (event.equals("END_ELEMENT")) {
				if (r.getLocalName().equals("record")) 
					return rec;
			}
			if (event.equals("START_ELEMENT")) {
				if (r.getLocalName().equals("leader")) {
					rec.leader = r.getElementText();
				} else if (r.getLocalName().equals("controlfield")) {
					ControlField f = new ControlField();
					f.id = ++id;
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("tag"))
							f.tag = r.getAttributeValue(i);
					f.value = r.getElementText();
					if (f.tag.equals("001"))
						rec.id = f.value;
					rec.control_fields.put(f.id, f);
				} else if (r.getLocalName().equals("datafield")) {
					DataField f = new DataField();
					f.id = ++id;
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("tag"))
							f.tag = r.getAttributeValue(i);
						else if (r.getAttributeLocalName(i).equals("ind1"))
							f.ind1 = r.getAttributeValue(i).charAt(0);
						else if (r.getAttributeLocalName(i).equals("ind2"))
							f.ind2 = r.getAttributeValue(i).charAt(0);
					f.subfields = processSubfields(r);
					rec.data_fields.put(f.id, f);
				}
		
			}
		}
		return rec;
	}
	
	private Map<Integer,Subfield> processSubfields( XMLStreamReader r ) throws Exception {
		Map<Integer,Subfield> fields = new HashMap<Integer,Subfield>();
		int id = 0;
		while (r.hasNext()) {
			String event = getEventTypeString(r.next());
			if (event.equals("END_ELEMENT"))
				if (r.getLocalName().equals("datafield"))
					return fields;
			if (event.equals("START_ELEMENT"))
				if (r.getLocalName().equals("subfield")) {
					Subfield f = new Subfield();
					f.id = ++id;
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("code"))
							f.code = r.getAttributeValue(i).charAt(0);
					f.value = r.getElementText();
					fields.put(f.id, f);
				}
		}
		return fields; // We should never reach this line.
	}
	
	private final static String getEventTypeString(int  eventType)
	{
	  switch  (eventType)
	    {
	        case XMLEvent.START_ELEMENT:
	          return "START_ELEMENT";
	        case XMLEvent.END_ELEMENT:
	          return "END_ELEMENT";
	        case XMLEvent.PROCESSING_INSTRUCTION:
	          return "PROCESSING_INSTRUCTION";
	        case XMLEvent.CHARACTERS:
	          return "CHARACTERS";
	        case XMLEvent.COMMENT:
	          return "COMMENT";
	        case XMLEvent.START_DOCUMENT:
	          return "START_DOCUMENT";
	        case XMLEvent.END_DOCUMENT:
	          return "END_DOCUMENT";
	        case XMLEvent.ENTITY_REFERENCE:
	          return "ENTITY_REFERENCE";
	        case XMLEvent.ATTRIBUTE:
	          return "ATTRIBUTE";
	        case XMLEvent.DTD:
	          return "DTD";
	        case XMLEvent.CDATA:
	          return "CDATA";
	        case XMLEvent.SPACE:
	          return "SPACE";
	    }
	  return  "UNKNOWN_EVENT_TYPE ,   "+ eventType;
	  
	  
	}

	static class FieldStats {
		public String tag;
		public Long recordCount = new Long(0);
		public Long instanceCount = new Long(0);

		// tabulating how many of a particular field appear in a record
		public Map<Integer,Integer> countByCount = new HashMap<Integer,Integer>();
		public Map<Integer,Integer> exampleByCount = new HashMap<Integer,Integer>();

		// tabulating frequency of particular indicator values
		public Map<Character,Integer> countBy1st = new HashMap<Character,Integer>();
		public Map<Character,Integer> exampleBy1st = new HashMap<Character,Integer>();
		public Map<Character,Integer> countBy2nd = new HashMap<Character,Integer>();
		public Map<Character,Integer> exampleBy2nd = new HashMap<Character,Integer>();
		public Map<String,Integer> countByBoth = new HashMap<String,Integer>();
		public Map<String,Integer> exampleByBoth = new HashMap<String,Integer>();
		
		// tabulating frequency of subfields
		public Map<Character,SubfieldStats> subfieldStatsByCode = new HashMap<Character,SubfieldStats>();
		
		// tabulating frequency of subfield pattern
		public Map<String,Integer> countBySubfieldPattern = new HashMap<String,Integer>();
		public Map<String,Integer> exampleBySubfieldPattern = new HashMap<String,Integer>();
		
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("Tag: "+this.tag+ " ("+ this.instanceCount+ " instances in "  +this.recordCount + " ("+
					(double)Math.round(1000 * this.recordCount / (double) MarcXmlToNTriples.recordCount)/10 +"%) records)\nField Frequencies: ");
			Integer[] fcounts = this.countByCount.keySet().toArray(
					                                   new Integer[ this.countByCount.keySet().size() ]);
			Arrays.sort( fcounts );
			for (Integer count: fcounts ) {
				sb.append("\n   "+count + " instance(s) of the field occurred in " + 
						this.countByCount.get(count) + " record(s). (Example record id: " +
						this.exampleByCount.get(count) + ")");
			}
			
			if (! this.countBy1st.isEmpty()) {
				sb.append("\n First Indicators: ");
				Character[] inds = this.countBy1st.keySet().toArray(
						                                     new Character[ this.countBy1st.keySet().size() ]);
				Arrays.sort( inds );
				for (Character ind: inds ) {
					sb.append("\n   \""+ ind + "\" occurred in " + this.countBy1st.get(ind) + 
							" field(s). (Example record id: " +  this.exampleBy1st.get(ind) + ")");
				}
			}
			
			if (! this.countBy2nd.isEmpty()) {
				sb.append("\n Second Indicators: ");
				Character[] inds = this.countBy2nd.keySet().toArray(
	                    new Character[ this.countBy2nd.keySet().size() ]);
				Arrays.sort( inds );
				for (Character ind: inds) {
					sb.append("\n   \""+ind + "\" occurred in " + this.countBy2nd.get(ind) + 
							" field(s). (Example record id: " +  this.exampleBy2nd.get(ind) + ")");
				}
			}
			
			if (! this.countByBoth.isEmpty()) {
				sb.append("\n Pairs of Indicators: ");
				String[] indpairs = this.countByBoth.keySet().toArray(
	                    new String[ this.countByBoth.keySet().size() ]);
				Arrays.sort( indpairs );
				for (String indpair: indpairs) {
					sb.append("\n   \""+indpair + "\" occurred in " + this.countByBoth.get(indpair) + 
							" field(s). (Example record id: " +  this.exampleByBoth.get(indpair) + ")");
				}
			}
				
			if (! this.countBySubfieldPattern.isEmpty()) {
				sb.append("\n Subfield Patterns: ");
				String[] s = this.countBySubfieldPattern.keySet().toArray(
	                    new String[ this.countBySubfieldPattern.keySet().size() ]);
				Arrays.sort( s );
				for (String subs: s) {
					sb.append("\n   \""+subs + "\" occurred in " + this.countBySubfieldPattern.get(subs) +
							" field(s). (Example record id: " + this.exampleBySubfieldPattern.get(subs) + ")");
				}
			}
				
/*			sb.append("\n Specific Subfields: \n");
			Character[] codes = this.subfieldStatsByCode.keySet().toArray(
                    new Character[ this.subfieldStatsByCode.keySet().size() ]);
			Arrays.sort( codes );
			for (Character code: codes) {
				sb.append(this.subfieldStatsByCode.get(code).toString());
			} */
			
			sb.append('\n');
			return sb.toString();
		}
		
	}
	static class SubfieldStats {
		public Character code;
		public Integer fieldCount = 0;
		public Long recordCount = new Long(0);
		public Long instanceCount = new Long(0);
		
		// tabulating how many of a particular subfield appear in a field
		public Map<Integer,Integer> countByCount = new HashMap<Integer,Integer>();
		public Map<Integer,Integer> exampleByCount = new HashMap<Integer,Integer>();
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(" Code: "+this.code+ " ("+this.instanceCount + " instances in " + this.fieldCount + 
					" fields in " + this.recordCount + " records)\n  Subfield Frequencies: ");
			Integer[] sfcounts = this.countByCount.keySet().toArray(
                    new Integer[ this.countByCount.keySet().size() ]);
			Arrays.sort( sfcounts );

			for (Integer count: sfcounts) {
				sb.append(count + "(" + this.countByCount.get(count) + "/" + this.exampleByCount.get(count)
						+ ") ");
			}
			sb.append("\n");
			return sb.toString();
		}
	}
	
	public static enum Mode {
		NAME_AS_SOURCE, RECORD_COUNT_BATCHES, ID_RANGE_BATCHES
	}

	public static enum Report {
		GEN_FREQ_BIB, GEN_FREQ_MFHD, 
		QC_880, QC_245, QC_SUBFIELD_CODES, QC_CJK_LABELING,
		EXTRACT_LOC_CLUES
	}

	
}
