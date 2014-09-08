package edu.cornell.library.integration.marcXmlToRdf;

import static edu.cornell.library.integration.ilcommons.util.CharacterSetUtils.hasCJK;
import static edu.cornell.library.integration.ilcommons.util.CharacterSetUtils.isCJK;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.ControlField;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.RecordType;
import edu.cornell.library.integration.indexer.MarcRecord.Subfield;
import edu.cornell.library.integration.indexer.utilities.IndexingUtilities;

//TODO: The coding for individual files as src or dest material is 
// incomplete and untested where it exists.
public class MarcXmlToRdf {
	
	private static Boolean debug = true;
	
	private static String logfile = "xmltordf.log";
	private static BufferedWriter logout;
	private static Integer groupsize = 1000;
	private OutputFormat outFormat = OutputFormat.NT_GZ;
	private String outFileExt = ".nt.gz";
	
	private Mode mode;
	private Boolean simultaneousWrite = true;
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
	private String currentInputFile = null;
	private String currentOutputFile = null;
	private BufferedOutputStream singleOut = null;
	private Map<Integer, BufferedOutputStream> outsById = null;
	private Map<String, BufferedOutputStream> outsByName = null;
	private String uriPrefix = null;
	private String idPrefix = null;
	private Boolean processingBibs = null;
	private Boolean processingMfhds = null;
	private Collection<Integer> foundBibs = new HashSet<Integer>();
	private Collection<Integer> foundMfhds = new HashSet<Integer>();
	
	// Reports fields
	private Collection<Report> reports = new HashSet<Report>();
	private Map<Report,String> reportResults = new HashMap<Report,String>();
	private Map<String,FieldStats> fieldStatsByTag = new HashMap<String,FieldStats>();
	private static Long recordCount = new Long(0);
	private Collection<Integer> no245a = new HashSet<Integer>();
	private Map<String,String> extractVals = null;
	private String outputHeaders = null;
	private int extractCols = 0;
	
	public MarcXmlToRdf(MarcXmlToRdf.Mode m) {
		mode = m;
	}

	/**
	 * Default output format is gzipped N-Triples (.nt.gz). Also available is a simpler
	 * text-formatted representation of the MARC records (.txt.gz).
	 * @param f
	 */
	public void setOutputFormat( OutputFormat f ) {
		outFormat = f;
		outFileExt = "." + f.toString().toLowerCase().replaceAll("_", ".");
	}
	
	/**
	 * Default output format is gzipped N-Triples (.nt.gz). Also available is a simpler
	 * text-formatted representation of the MARC records (.txt.gz).
	 *
	 * Deactivate simultaneous output handlers for multi-file output. Not recommended
	 * for gzipped output files, as re-opening and appending to an existing .gz file
	 * may result in an encoding not supported by all readers (including Jena).
	 * 
	 * CURRENTLY ONLY SUPPORTED FOR Mode.ID_RANGE_BATCHES
	 * DEFINITELY NOT SUPPORTED FOR N-Triples OUTPUT!
	 */
	public void setOutputFormatWithoutSimultaneousWrite( OutputFormat f ) {
		simultaneousWrite = false;
		outFormat = f;
		if (f.toString().endsWith("GZ")) {
			outFileExt = "." + f.toString().toLowerCase().replaceAll("_", ".").replaceAll(".gz$", "");
		} else 
			outFileExt = "." + f.toString().toLowerCase().replaceAll("_", ".");
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
	
	public void setIdPrefix( String p ) {
		idPrefix = p;
	}
	/**
	 * add report to desired reports
	 * @param r
	 */
	public void addReport( Report r ) {
		reports.add(r);
		if (r.toString().startsWith("EXTRACT_"))
			populateExtractHeaders( r );
	}
	
	public String getReport( Report r ) {
		if (reportResults.containsKey(r)) 
			return reportResults.get(r);
		return null;
	}
	
	public void run() throws Exception {

		validateRun();
		
		if (simultaneousWrite)
			if (mode.equals(Mode.NAME_AS_SOURCE))
				outsByName = new HashMap<String,BufferedOutputStream>();
			else
				outsById = new HashMap<Integer, BufferedOutputStream>();
		
		// If the destination is local, we can build N-Triples directly to there.
		// Otherwise, we need a temporary build directory.
		if (isDestDav) {
			tempDestDir = Files.createTempDirectory(Paths.get(""),"IL-xml2NT-").toString();
			if (debug) System.out.println(tempDestDir);
		}
		
		// If we want RECORD_COUNT_BATCHES, we will need to process bibs twice,
		// so downloading in advance will help.
		if (mode.equals(Mode.RECORD_COUNT_BATCHES) && isBibSrcDav)
			tempBibSrcDir = downloadBibsToTempDir();
		
		// ID_RANGE_BATCHES don't require precalculated ranges, but RECORD_COUNT_BATCHES do.
		if (mode.equals(Mode.RECORD_COUNT_BATCHES))
			determineTargetBatches();
		
		if (processingBibs) processBibs(); foundBibs.clear();
		if (reports.contains(Report.GEN_FREQ_BIB))
			reportResults.put(Report.GEN_FREQ_BIB,buildGenFreqReport());

		if (processingMfhds) processMfhds(); foundMfhds.clear();
		if (reports.contains(Report.GEN_FREQ_MFHD))
			reportResults.put(Report.GEN_FREQ_MFHD,buildGenFreqReport());
		
		if (outsByName != null)
			for (BufferedOutputStream out : outsByName.values())
				out.close();
		if (outsById != null)
			for (BufferedOutputStream out : outsById.values())
				out.close();
		if (singleOut != null)
			singleOut.close();
		
		if (! simultaneousWrite && outFormat.toString().endsWith("GZ")) {
			DirectoryStream<Path> stream = Files.newDirectoryStream(
					Paths.get(tempDestDir));
			for (Path file: stream) {
				if (file.endsWith(".gz")) continue;
				if (debug) System.out.println("gzipping "+file);
				IndexingUtilities.gzipFile(file.toString(),file.toString()+".gz");
			}

		}

		if (isDestDav) uploadOutput();
		
		if (tempDestDir != null)
			FileUtils.deleteDirectory(new File(tempDestDir));
		if (tempBibSrcDir != null)
			FileUtils.deleteDirectory(new File(tempBibSrcDir));
		
	}
	
	private void uploadOutput() throws Exception {
		
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
	
	private BufferedOutputStream openFileForWrite( String f ) throws Exception {
		BufferedOutputStream b = null;
		if (outFileExt.endsWith(".gz")) 
			b =  new BufferedOutputStream(new GZIPOutputStream(
					new FileOutputStream(f,true)));
		else {
			b =  new BufferedOutputStream(new FileOutputStream(f,true));
		}
		return b;
	}

	private void sortOutput( String bibid, String output ) throws Exception {
		BufferedOutputStream out = null;
		String dirToProcessInto = null;
		boolean newOut = false;
		
		if (tempDestDir != null)
			dirToProcessInto = tempDestDir;
		else if (destDir != null && isDestDav.equals(false))
			dirToProcessInto = destDir;
		
		if (mode.equals(Mode.ID_RANGE_BATCHES)) {
			Integer batchid = Integer.valueOf(bibid) / groupsize;
			String file = dirToProcessInto+"/"+destFilenamePrefix+"."+batchid+outFileExt;
			if (simultaneousWrite) {
				if ( ! outsById.containsKey(batchid)) {
					out = openFileForWrite(file);
					newOut = true;
					outsById.put(batchid, out);
				} else 
					out = outsById.get(batchid);
			} else {
				if (file.equals(currentOutputFile)) {
					out = singleOut;
				} else {
					if (singleOut != null)
						singleOut.close();
					File f = new File(file);
					if (! f.exists())
						newOut = true;
					out = openFileForWrite(file);
					singleOut = out;
					currentOutputFile = file;
				}
			}

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
			if (outsByName.containsKey(currentInputFile)) {
				out = outsByName.get(currentInputFile);
			} else {
				if (dirToProcessInto != null) {
					String targetFile = (destFilenamePrefix != null) 
							? dirToProcessInto+"/"+destFilenamePrefix+"."+swapFileExt(currentInputFile)
									: dirToProcessInto+"/"+swapFileExt(currentInputFile);
					if (debug)
						System.out.println("Opening output handle for "+targetFile);
					out = openFileForWrite(targetFile);
				} else {
					// dest is a file, and not on dav
					out = openFileForWrite(dirToProcessInto+"/"+ swapFileExt(currentInputFile));
				}
				newOut = true;
				outsByName.put(currentInputFile, out);
			}
		}
		
		if (out != null) {
			if (newOut && (outFormat.equals(OutputFormat.TDF)
					|| outFormat.equals(OutputFormat.TDF_GZ)
					|| outFormat.equals(OutputFormat.N3)
					|| outFormat.equals(OutputFormat.N3_GZ)
					))
				out.write( outputHeaders.getBytes() );
			out.write( output.getBytes() );
		} else {
			System.out.println("N-Triples not written to file. Bibid: "+bibid);
		}
	}
	
	public String swapFileExt ( String xml ) {
		String nt;
		if (xml.endsWith(".xml"))
			nt = xml.substring(0,xml.length()-4)+outFileExt;
		else
			nt = xml + outFileExt;
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

					if (type.equals(RecordType.BIBLIOGRAPHIC)) {
						if (foundBibs.contains(Integer.valueOf(rec.id))) {
							System.out.println("Skipping duplicate bib record: "+rec.id);
							continue;
						} else foundBibs.add(Integer.valueOf(rec.id));
					} else if (type.equals(RecordType.HOLDINGS)) {
						if (foundMfhds.contains(Integer.valueOf(rec.id))) {
							System.out.println("Skipping duplicate holding record: "+rec.id);
							continue;
						} else foundMfhds.add(Integer.valueOf(rec.id));
					}
					
					if (isSuppressionBlocked(rec.id, type))
						continue;

					mapNonRomanFieldsToRomanizedFields(rec);
					extractData(rec);
					if (reports.contains(Report.QC_CJK_LABELING))
						surveyForCJKValues(rec);
					if ((type.equals(RecordType.BIBLIOGRAPHIC) && reports.contains(Report.GEN_FREQ_BIB))
							|| (type.equals(RecordType.HOLDINGS) && reports.contains(Report.GEN_FREQ_MFHD))) {
						tabulateFieldData(rec);
					}
					String output = null;
					if (outFormat.equals(OutputFormat.NT_GZ)) {
						output = generateNTriples( rec );
					} else if (outFormat.equals(OutputFormat.TXT_GZ))
						output = rec.toString() + "\n";
					else if (outFormat.equals(OutputFormat.TDF) || outFormat.equals(OutputFormat.TDF_GZ))
						output = compileExtractReport();
					else if (outFormat.equals(OutputFormat.N3) || outFormat.equals(OutputFormat.N3_GZ))
						output = generateN3(rec);

					if (type.equals(RecordType.BIBLIOGRAPHIC))
						sortOutput(rec.id,output);
					else if (type.equals(RecordType.HOLDINGS))
						sortOutput(rec.bib_id,output);
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
				currentInputFile = file.toString().substring(
						file.toString().lastIndexOf(File.separator)+1);
				if (debug) System.out.println(file);
				readXml(new FileInputStream(file.toString()),
						RecordType.BIBLIOGRAPHIC );
			}
			return;
		}
		
		if (localProcessFile != null) {
			currentInputFile = localProcessFile.substring(
					localProcessFile.lastIndexOf(File.separator)+1);
			if (debug) System.out.println(localProcessFile);
			readXml(new FileInputStream(localProcessFile),
					RecordType.BIBLIOGRAPHIC );
			return;
		}
		
		// At this point we're looking for Dav sources
		if (bibSrcDir != null) {
			List<String> files = bibSrcDav.getFileUrlList(bibSrcDir);
			for ( String file : files) {
				currentInputFile = file.substring(file.lastIndexOf('/')+1);
				if (debug) System.out.println(file);
				readXml(bibSrcDav.getFileAsInputStream(file),
						RecordType.BIBLIOGRAPHIC );
			}
			return;
		}
		
		if (bibSrcFile != null) {
			currentInputFile = bibSrcFile.substring(bibSrcFile.lastIndexOf('/')+1);
			if (debug) System.out.println(bibSrcFile);
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
				currentInputFile = file.toString().substring(
						file.toString().lastIndexOf(File.separator)+1);
				if (debug) System.out.println(file);
				readXml(new FileInputStream(file.toString()),
						RecordType.HOLDINGS );
			}
			return;
		}
		
		if (localProcessFile != null) {
			currentInputFile = localProcessFile.substring(
					localProcessFile.lastIndexOf(File.separator)+1);
			if (debug) System.out.println(localProcessFile);
			readXml(new FileInputStream(localProcessFile),
					RecordType.HOLDINGS );
			return;
		}

		// At this point we're looking for Dav sources
		if (mfhdSrcDir != null) {
			List<String> files = mfhdSrcDav.getFileUrlList(mfhdSrcDir);
			for ( String file : files) {
				currentInputFile = file.substring(file.lastIndexOf('/')+1);
				if (debug) System.out.println(file);
				readXml(mfhdSrcDav.getFileAsInputStream(file),
						RecordType.HOLDINGS );
			}
			return;
		}
		
		if (mfhdSrcFile != null) {
			currentInputFile = mfhdSrcFile.substring(mfhdSrcFile.lastIndexOf('/')+1);
			if (debug) System.out.println(mfhdSrcFile);
			readXml(mfhdSrcDav.getFileAsInputStream(mfhdSrcFile),
					RecordType.HOLDINGS );
			return;
		}
	}

	private void determineTargetBatches() throws Exception {
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
			BufferedOutputStream  out = openFileForWrite(dirToProcessInto+"/"+
					destFilenamePrefix+"."+i3+outFileExt);
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
		Path tempLocalBibDir = Files.createTempDirectory(Paths.get(""), "IL-xml2NT-");
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
	
	/*
	private static Pattern shadowLinkPattern 
	   = Pattern.compile("https?://catalog.library.cornell.edu/cgi-bin/Pwebrecon.cgi\\?BBID=([0-9]+)&DB=local");
	private static Collection<String> shadowLinkedRecs = new HashSet<String>();
	*/

	private void surveyForCJKValues( MarcRecord rec ) throws IOException {
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
	
	
/*	
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
	
	private String compileExtractReport( ) {
		String[] values = new String[extractCols];
		for (int i = 0; i < extractCols; i++) values[i] = "";
		for (String key : extractVals.keySet())
			values[Integer.valueOf(key)-1] = extractVals.get(key);
		return StringUtils.join(values, '\t')+"\n";
	}
	
	private void populateExtractHeaders( Report rep ) {
		List<String> v = new ArrayList<String>();
		if (rep.equals(Report.EXTRACT_LANGUAGE)) {
			v.add("id");
			v.add("leader");
			v.add("008");
			v.add("040");
			v.add("008/35-37");
			v.add("041");
			v.add("500a");
			v.add("546a");
		} else if (rep.equals(Report.EXTRACT_PUBPLACE)) {
			v.add("id");
			v.add("leader");
			v.add("008");
			v.add("040");
			v.add("008/15-17");
			v.add("020az");
			v.add("1st 260a/246a");
			v.add("Other 260a/264a");
		} else if (rep.equals(Report.EXTRACT_SUBJPLACE)) {
			v.add("id");
			v.add("leader");
			v.add("008");
			v.add("040");
			v.add("043a");
			v.add("050a/090a");
			v.add("505a");
			v.add("520a");
			v.add("6XXz");
			v.add("651a");
		}
		outputHeaders = StringUtils.join(v,"\t")+"\n";
		extractCols = v.size();
	}
	
	/**
	 * Extract Data for reports of extract reports. (Extract reports must be run
	 * in with OutputFormat.TDF_GZ rather than NT_GZ.)
	 * @param rec
	 * @throws Exception
	 */
	private void extractData( MarcRecord rec ) throws Exception {

		Boolean isPubPlace = reports.contains(Report.EXTRACT_PUBPLACE);
		Boolean isSubjPlace = reports.contains(Report.EXTRACT_SUBJPLACE);
		Boolean isLanguage = reports.contains(Report.EXTRACT_LANGUAGE);
		Boolean isTitleMatch = reports.contains(Report.EXTRACT_TITLE_MATCH);
		
		if ( ! isPubPlace
				&& ! isTitleMatch
				&& ! isSubjPlace
				&& ! isLanguage)
			return;

		List<String> pubplaces = new ArrayList<String>();
		
		if (extractVals != null)
			extractVals.clear();
		else
			extractVals = new HashMap<String,String>();
			
		if (isPubPlace || isSubjPlace || isLanguage)
			extractVals.put("02", rec.leader);
		
		for (Integer fid : rec.control_fields.keySet()) {
			ControlField f = rec.control_fields.get(fid);
			
			if (f.tag.equals("001")) 
				extractVals.put("01", f.value);
			
			if (f.tag.equals("008")) {

				if (isPubPlace || isSubjPlace || isLanguage)
					extractVals.put("03", f.value);
				
				if (isPubPlace)
					if (f.value.length() >= 18)
						extractVals.put("05", f.value.substring(15, 18));

				if (isLanguage)
					if (f.value.length() >= 38)
						extractVals.put("05", f.value.substring(35,38));
			}
	
		}
				
		for (Integer fid: rec.data_fields.keySet()) {
			DataField f = rec.data_fields.get(fid);

			
			if (isPubPlace)
				if (f.tag.equals("020"))
					for (Subfield sf : f.subfields.values()) 
						if (sf.code.equals('a') || sf.code.equals('z') )
							putOrAppendToExtract("06", " ", sf.toString('$'));

			if (isPubPlace || isSubjPlace || isLanguage)
				if (f.tag.equals("040"))
					putOrAppendToExtract("04","; ",f.toString('$'));

			if (isLanguage)
				if (f.tag.equals("041"))
					putOrAppendToExtract("06", "; ", f.toString('$'));

			if (isSubjPlace)
				if (f.tag.equals("043"))
					putOrAppendToExtract("05"," ",f.concatenateSpecificSubfields("a"));
			
			if (isSubjPlace)
				if (f.tag.equals("050") || f.tag.equals("090"))
					putOrAppendToExtract("06","; ",f.tag+" "+f.concatenateSpecificSubfields("a"));
			
			if (isPubPlace)
				if (f.tag.equals("260") || f.tag.equals("264"))
					pubplaces.add(f.tag + " " + f.concatenateSpecificSubfields("a"));
			
			if (isLanguage)
				if (f.tag.equals("500"))
					putOrAppendToExtract("07","; ",f.concatenateSpecificSubfields("a"));
			
			if (isSubjPlace)
				if (f.tag.equals("505"))
					putOrAppendToExtract("07","; ",f.concatenateSpecificSubfields("a"));
				
			if (isSubjPlace)
				if (f.tag.equals("520"))
					putOrAppendToExtract("08","; ",f.concatenateSpecificSubfields("a"));
				
			if (isLanguage)
				if (f.tag.equals("546"))
					putOrAppendToExtract("08","; ",f.concatenateSpecificSubfields("a"));

			if (isSubjPlace)
				if (f.tag.startsWith("6"))
					for (Subfield sf : f.subfields.values())
						if (sf.code.equals('z'))
							putOrAppendToExtract("09","; ",f.tag+" "+sf.value);
			
			if (isSubjPlace)
				if (f.tag.equals("651"))
					putOrAppendToExtract("10","; ",f.concatenateSpecificSubfields("a"));
			
		}
		
		if (pubplaces.size() > 0) {
			extractVals.put("07",pubplaces.get(0));
			if (pubplaces.size() > 1) {
				pubplaces.remove(0);
				extractVals.put("08", StringUtils.join(pubplaces,"; "));
			}
		}
	
	}
	
	private void putOrAppendToExtract(String key, String joinWith, String val) {
		if (extractVals.containsKey(key))
			extractVals.put(key, extractVals.get(key) + joinWith + val);
		else 
			extractVals.put(key,val);
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
	
	
	private String generateN3 ( MarcRecord rec ) {
		Model model = marcRecordToJenaModel( rec );
		StringWriter out = new StringWriter();
		RDFDataMgr.write(out, model, Lang.N3);
		model.close();
		String n3 = out.toString();
		int headerOffset = n3.indexOf("\n\n");
		if (outputHeaders == null)
			outputHeaders = n3.substring(0, headerOffset)+"\n";
		return n3.substring(headerOffset+1);
	}

	private String generateNTriples ( MarcRecord rec ) {
		Model model = marcRecordToJenaModel( rec );
		StringWriter out = new StringWriter();
		RDFDataMgr.write(out, model, Lang.NT);
		model.close();
		return out.toString();
	}
	
	private Model marcRecordToJenaModel (MarcRecord rec) {
		String id = rec.control_fields.get(1).value;
		rec.id = id;
		Model model = ModelFactory.createMemModelMaker().createDefaultModel();
		String marcrdf = "http://marcrdf.library.cornell.edu/canonical/0.1/";
		String uri_host = "http://da-rdf.library.cornell.edu/individual/";
		if (uriPrefix != null) 
			uri_host = uriPrefix;

		Resource recType_res = null;		
		String id_pref;
		if (rec.type == RecordType.BIBLIOGRAPHIC) {
			id_pref = "b";
			recType_res = model.createResource("http://marcrdf.library.cornell.edu/canonical/0.1/BibliographicRecord");
		} else if (rec.type == RecordType.HOLDINGS) {
			id_pref = "h";
			recType_res = model.createResource("http://marcrdf.library.cornell.edu/canonical/0.1/HoldingsRecord");
		} else { //if (type == RecordType.AUTHORITY) {
			id_pref = "a";
			recType_res = model.createResource("http://marcrdf.library.cornell.edu/canonical/0.1/AuthorityRecord");
		}
		if (idPrefix != null) id_pref = idPrefix;
//		Property type_p = model.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
		Property type_p = model.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#","type");

		Resource rec_res = model.createResource(uri_host+id_pref+id);
		rec_res.addProperty(type_p,recType_res);
		
		Property label_p = model.createProperty("http://www.w3.org/2000/01/rdf-schema#label");
		if (idPrefix != null) 
			rec_res.addLiteral(label_p,idPrefix+"_"+ rec.id);
		else 
			rec_res.addLiteral(label_p, rec.id);
		if (rec.type.equals(RecordType.BIBLIOGRAPHIC) && isUnsuppressedBibListFlagged)
			rec_res.addLiteral(model.createProperty(marcrdf,"status"),
					(unsuppressedBibs.contains(id))?"unsuppressed":"suppressed");
		if (rec.type.equals(RecordType.HOLDINGS) && isUnsuppressedMfhdListFlagged)
			rec_res.addLiteral(model.createProperty(marcrdf,"status"),
					(unsuppressedMfhds.contains(id))?"unsuppressed":"suppressed");
		rec_res.addLiteral(model.createProperty("http://marcrdf.library.cornell.edu/canonical/0.1/leader"), rec.leader);

		// Control fields
		int fid = 0;
		Resource controlFieldType_res = model.createResource("http://marcrdf.library.cornell.edu/canonical/0.1/ControlField");
		Property tag_p = model.createProperty(marcrdf,"tag");
		Property value_p = model.createProperty(marcrdf,"value");
		while( rec.control_fields.containsKey(fid+1) ) {
			ControlField f = rec.control_fields.get(++fid);
			Resource field_res = model.createResource(uri_host+id_pref+id+"_"+fid);
			rec_res.addProperty(model.createProperty(marcrdf,"hasField"+f.tag),field_res);
			field_res.addProperty(type_p,controlFieldType_res);
			field_res.addLiteral(tag_p, f.tag);
			if (f.tag.equals("001") && idPrefix != null) {
				field_res.addLiteral(value_p, idPrefix +"_"+ f.value);
			} else {
				field_res.addLiteral(value_p, f.value);
			}
			if ((f.tag.contentEquals("004")) && (rec.type == RecordType.HOLDINGS)) {
				rec.bib_id = f.value;
				rec_res.addProperty(model.createProperty(marcrdf,"hasBibliographicRecord"),
						model.createResource(uri_host+"b"+f.value));
			}
		}

		Resource dataFieldType_res = model.createResource("http://marcrdf.library.cornell.edu/canonical/0.1/DataField");
		Resource subfieldType_res = model.createResource("http://marcrdf.library.cornell.edu/canonical/0.1/Subfield");
		Property hasSF_p = model.createProperty(marcrdf,"hasSubfield");
		Property ind1_p = model.createProperty(marcrdf,"ind1");
		Property ind2_p = model.createProperty(marcrdf,"ind2");
		Property code_p = model.createProperty(marcrdf,"code");
		while( rec.data_fields.containsKey(fid+1) ) {
			DataField f = rec.data_fields.get(++fid);
			Resource field_res = model.createResource(uri_host+id_pref+id+"_"+fid);
			rec_res.addProperty(model.createProperty(marcrdf,"hasField"+f.tag),field_res);
			if (f.alttag != null)
				rec_res.addProperty(model.createProperty(marcrdf,"hasField"+f.alttag),field_res);
			field_res.addProperty(type_p, dataFieldType_res);
			field_res.addLiteral(tag_p, f.tag);
			field_res.addLiteral(ind1_p, f.ind1);
			field_res.addLiteral(ind2_p, f.ind2);

			int sfid = 0;
			while( f.subfields.containsKey(sfid+1) ) {
				Subfield sf = f.subfields.get(++sfid);
				Resource sf_res = model.createResource(uri_host+id_pref+id+"_"+fid+"_"+sfid);
				field_res.addProperty(hasSF_p, sf_res);
				sf_res.addProperty(type_p, subfieldType_res);
				sf_res.addLiteral(code_p, sf.code);
				sf_res.addLiteral(value_p, sf.value);
			}

		}
		model.setNsPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
		model.setNsPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
		model.setNsPrefix("marcrdf",marcrdf);
		if (idPrefix != null)
			model.setNsPrefix(idPrefix, uri_host);
		else 
			model.setNsPrefix("voyager", uri_host);
		model.setNsPrefix("xml", "http://www.w3.org/2001/XMLSchema#");
		return model;
	}
/*	
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
	} */

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
					(double)Math.round(1000 * this.recordCount / (double) MarcXmlToRdf.recordCount)/10 +"%) records)\nField Frequencies: ");
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

	public static enum OutputFormat {
		NT_GZ, TXT_GZ, TDF, TDF_GZ, N3, N3_GZ
	}

	public static enum Report {
		GEN_FREQ_BIB, GEN_FREQ_MFHD, 
		QC_880, QC_245, QC_SUBFIELD_CODES, QC_CJK_LABELING,
		EXTRACT_PUBPLACE, EXTRACT_SUBJPLACE, EXTRACT_LANGUAGE,
		EXTRACT_TITLE_MATCH
	}

	
}
