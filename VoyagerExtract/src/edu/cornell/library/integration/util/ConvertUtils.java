package edu.cornell.library.integration.util; 

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream; 
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern; 

import org.apache.commons.io.FileUtils; 
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.marc4j.MarcException;
import org.marc4j.MarcPermissiveStreamReader;
import org.marc4j.MarcXmlWriter;
import org.marc4j.converter.CharConverter;
import org.marc4j.converter.impl.AnselToUnicode;
import org.marc4j.converter.impl.Iso5426ToUnicode;
import org.marc4j.converter.impl.Iso6937ToUnicode;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.DataField;
import org.marc4j.marc.Record;
import org.marc4j.marc.Subfield;
import org.marc4j.marc.impl.LeaderImpl;
import org.marc4j.marc.impl.SubfieldImpl;

import edu.cornell.library.integration.ilcommons.service.DavService;

public class ConvertUtils {
   
   protected final Log logger = LogFactory.getLog(getClass());
   
   /** The range of non allowable characters in XML 1.0 (ASCII Control Characters) */
   private static final String WEIRD_CHARACTERS =
      "[\u0001-\u0008\u000b-\u000c\u000e-\u001f]";
   private static final Pattern WEIRD_CHARACTERS_PATTERN =
         Pattern.compile(WEIRD_CHARACTERS);
   public static final String LN = System.getProperty("line.separator");
   
   public static final String TMPDIR = "/tmp";
   
   /** the weird character matcher */
   private Matcher matcher;
   
   private  String controlNumberOfLastReadRecord = null;
   
   /** MARC-8 ANSEL ENCODING **/
   public  final String MARC_8_ENCODING = "MARC8";
   
   /** ISO5426 ENCODING **/
   public  final String ISO5426_ENCODING = "ISO5426";

   /** ISO6937 ENCODING **/
   public  final String ISO6937_ENCODING = "ISO6937";
   
   private  String convertEncoding = null;
   
   /** perform Unicode normalization */
   private  boolean normalize = true;
   
   /** split output file into this many records */
   private int splitSize = 10000;
   
   /** prefix to use in sequence string in output file name */
   private String sequence_prefix = "0";
   
   /** holds a bibid, mfid, or other unique id for single or updates conversions */
   private String itemId = "";
   
   /** timestamp string from the original source file */
   private String ts;
   
   /** the type of marc21 records, typically bib or mfhd */
   private String srcType;
   
   /** the type of extract: full, daily, or updates*/
   private String extractType;
   
   /** destination DAV directory to save XML */
   private String destDir;

   public ConvertUtils() {
      // TODO Auto-generated constructor stub
   }
   
   /**
    * @param normalize
    */
   public void setNormalize(boolean normalize) {
      this.normalize = normalize;
   }

   /**
    * @return
    */
   public int getSplitSize() {
      return splitSize;
   }

   /**
    * @param splitSize
    */
   public void setSplitSize(int splitSize) {
      this.splitSize = splitSize;
   } 
   
   /**
    * @return the convertEncoding
    */
   public String getConvertEncoding() {
      return convertEncoding;
   }

   /**
    * @param convertEncoding the convertEncoding to set
    */
   public void setConvertEncoding(String convertEncoding) {
      this.convertEncoding = convertEncoding;
   }

   /**
    * @return the sequence_prefix
    */
   public String getSequence_prefix() {
      return sequence_prefix;
   }

   /**
    * @param sequence_prefix the sequence_prefix to set
    */
   public void setSequence_prefix(String sequence_prefix) {
      this.sequence_prefix = sequence_prefix;
   }
   
   /**
    * @return the itemId
    */
   public String getItemId() {
      return itemId;
   }

   /**
    * @param itemId the itemId to set
    */
   public void setItemId(String itemId) {
      this.itemId = itemId;
   }

   /**
    * @return the srcType
    */
   public String getSrcType() {
      return srcType;
   }

   /**
    * @param srcType the srcType to set
    */
   public void setSrcType(String srcType) {
      this.srcType = srcType;
   }

   /**
    * @return the extractType
    */
   public String getExtractType() {
      return extractType;
   }

   /**
    * @param extractType the extractType to set
    */
   public void setExtractType(String extractType) {
      this.extractType = extractType;
   }

   /**
    * @return the ts
    */
   public String getTs() {
      return ts;
   }

   /**
    * @param ts the ts to set
    */
   public void setTs(String ts) {
      this.ts = ts;
   }

   /**
    * @return the destDir
    */
   public String getDestDir() {
      return destDir;
   }

   /**
    * @param destDir the destDir to set
    */
   public void setDestDir(String destDir) {
      this.destDir = destDir;
   }

   public String getControlNumberOfLastReadRecord() {
      return controlNumberOfLastReadRecord;
   } 
   
   /**
    * @param mrc
    * @param davService
    * @return
    * @throws Exception
    */
   public void convertMrcToXml(DavService davService, String srcDir, String srcFile) throws Exception {
       
      MarcXmlWriter writer = null;
      boolean hasInvalidChars;
      /** record counter */
      int counter = 0;
      int total = 0;
      int batch = 0;      
      Record record = null;
      String destXmlFile = new String();
      String tmpFilePath = TMPDIR +"/"+ srcFile;
      File f = davService.getFile(srcDir +"/"+ srcFile, tmpFilePath);
      FileInputStream is = new FileInputStream(f);
       
      MarcPermissiveStreamReader reader = null;
      boolean permissive      = true;
      boolean convertToUtf8   = true;
      reader = new MarcPermissiveStreamReader(is, permissive, convertToUtf8);
       
      destXmlFile = getOutputFileName(0);
      writer = getWriter(destXmlFile);       
      
      if (normalize == true) {
         writer.setUnicodeNormalization(true);
      }
       
      try {
         while (reader.hasNext()) {
            try {
               record = reader.next();
            } catch (MarcException me) {
               logger.error("MarcException reading record", me);
               continue;
            } catch (Exception e) {
               e.printStackTrace();
               continue;
            }
            counter++; 
            total++;
            controlNumberOfLastReadRecord = record.getControlNumber();
            if (MARC_8_ENCODING.equals(convertEncoding)) {
               record.getLeader().setCharCodingScheme('a');
            }
             
            hasInvalidChars = false;
            matcher = WEIRD_CHARACTERS_PATTERN.matcher(record.toString());
            if (matcher.find()) {
               hasInvalidChars = doReplacements(record, matcher);
            }
   
            if (!hasInvalidChars) {
               writer.write(record);
            }
            
            // check to see if we need to write out a batch of records
            if (splitSize > 0 && counter == splitSize) {
               
               
               System.out.println("\nsaving xml batch "+ destXmlFile);
               try {
                  if (writer != null) writer.close();                   
               } catch (Exception ex) {
                  logger.error("Could not close writer", ex);   
               }
               // move the XML to the DAV store and open a new writer
               moveXmlToDav(davService, destDir, destXmlFile);
               batch++; 
                
               destXmlFile = getOutputFileName(counter * batch);
               writer = getWriter(destXmlFile); 
               counter = 0;
            } // end writing batch   
             
         } // end while loop
         
         if (total > 0) {
            System.out.println("\nsaving final xml batch "+ destXmlFile);
         }
         try { 
            if (writer != null) writer.close();             
         } catch (Exception ex) {
            logger.error("could not close writer", ex);
         }
         if (total > 0) {
            moveXmlToDav(davService, destDir, destXmlFile);
         }
         
      } finally {
          
         try { 
            is.close();
         } catch (IOException e) {
            e.printStackTrace();
         } 
      }
      
      FileUtils.deleteQuietly(f);
      
      System.out.println("\ntotal record count: "+ total);          
      
       
   }
   
   /**
    * @param record
    * @param matcher
    * @return
    */
   private  boolean doReplacements(Record record, Matcher matcher) {
      boolean hasInvalidChars = true;
      String recordString = record.toString();

      //invalidCharIndex = matcher.start();
      List<Integer> invalidCharsIndex = new ArrayList<Integer>();
      do {
         invalidCharsIndex.add(matcher.start());
      } while(matcher.find());
      StringBuffer badCharLocator = new StringBuffer();
      List<String> invalidChars = new ArrayList<String>();
      for (Integer i : invalidCharsIndex) {
         RecordLine line = getLineOfRecord(recordString, i);
         badCharLocator.append(line.getErrorLocation()).append(LN);
         invalidChars.add(line.getInvalidChar() + " ("
               + line.getInvalidCharHexa() + ")" + " position: " + i);
         String invalidCharacter = (line.getInvalidChar() + " ("
               + line.getInvalidCharHexa() + ")" + "position: " + i);
         String badCharacterLocator = (line.getErrorLocation() + LN);
          
         modifyRecord(record, line, invalidCharacter, badCharacterLocator);
          
      }
      matcher = WEIRD_CHARACTERS_PATTERN.matcher(record.toString());
      hasInvalidChars = matcher.find() ? true : false;
       
      
      return hasInvalidChars;
   }
   
   /**
    * @param recordString
    * @param position
    * @return
    */
   private  RecordLine getLineOfRecord(String recordString, int position) {
      return new RecordLine(recordString, position);
   }
   
   /**
    * @param record
    * @param line
    * @param invalidCharacter
    * @param badCharacterLocator
    */
   private  void modifyRecord(Record record, RecordLine line,
         String invalidCharacter, String badCharacterLocator) {

      // change LEADER
      // String leaderReplaced = "The character is replaced with zero.\n";
      if (line.getLine().startsWith("LEADER")) {
         record.setLeader(new LeaderImpl(record.getLeader().toString()
               .replaceAll(WEIRD_CHARACTERS, "0")));
      }

      // change control fields 
      String NonleaderReplaced = "The character is replaced with space.\n";
      if (line.getLine().startsWith("00")) {
         String tag = line.getLine().substring(0, 3);
         ControlField fd = (ControlField) record.getVariableField(tag);
         fd.setData(fd.getData().replaceAll(WEIRD_CHARACTERS, " "));
         record.addVariableField(fd);

         // change data fields
      } else if (line.getLine().startsWith("LEADER") == false) {
         String tag = line.getLine().substring(0, 3);
         DataField fd = (DataField) record.getVariableField(tag);
         record.removeVariableField(fd);

         // indicators
         fd.setIndicator1(String.valueOf(fd.getIndicator1())
               .replaceAll(WEIRD_CHARACTERS, " ").charAt(0));
         fd.setIndicator2(String.valueOf(fd.getIndicator2())
               .replaceAll(WEIRD_CHARACTERS, " ").charAt(0));

         // subfields
         List<Subfield> sfs = fd.getSubfields();
         List<Subfield> newSfs = new ArrayList<Subfield>();
         List<Subfield> oldSfs = new ArrayList<Subfield>();
         // replace the subfields' weird characters
         for (Subfield sf : sfs) {
            oldSfs.add(sf);
            char code;
            if (WEIRD_CHARACTERS_PATTERN.matcher(
                  String.valueOf(sf.getCode())).find()) {
               code = String.valueOf(sf.getCode())
                     .replaceAll(WEIRD_CHARACTERS, " ").charAt(0);
            } else {
               code = sf.getCode();
            }
            newSfs.add(new SubfieldImpl(code, sf.getData().replaceAll(
                  WEIRD_CHARACTERS, " ")));
         }
         // remove old subfields ...
         for (Subfield sf : oldSfs) {
            fd.removeSubfield(sf);
         }
         // ... and add the new ones
         for (Subfield sf : newSfs) {
            fd.addSubfield(sf);
         }
         record.addVariableField(fd);

      }
       
   }
   
   // this method figures out what the output file name should be
   private String getOutputFileName(int batch) {
      StringBuffer sb = new StringBuffer();
      String sequence = new String();
      
      if (StringUtils.equals(getExtractType(), "single") ) {
         sb.append(getSrcType() +"."+ getTs() +"."+ getItemId() +".xml");
      } else if (StringUtils.equals(getExtractType(), "updates")) {
         sb.append(getSrcType() +"."+ getTs() +"."+ getItemId() +".xml");   
      } else if (StringUtils.equals(getExtractType(), "daily")) {
         if (batch == 0) {
            sequence = String.valueOf(getSequence_prefix()) +"_1";   
         } else {
            sequence = String.valueOf(getSequence_prefix()) +"_"+ String.valueOf(batch);
         }
         sb.append(getSrcType() +"."+ getTs() +"."+ sequence +".xml");
      } else if (StringUtils.equals(getExtractType(), "full")) {
         if (batch == 0) {
            sequence = String.valueOf(getSequence_prefix()) +"_1";   
         } else {
            sequence = String.valueOf(getSequence_prefix()) +"_"+ String.valueOf(batch);
         }
         sb.append(getSrcType() +"."+ getTs() +"."+ sequence +".xml");
      }
      
      return sb.toString();
   } 
   
   /**
    * @param davService
    * @param destDir
    * @param destXmlFile
    * @throws Exception
    */
   private void moveXmlToDav(DavService davService, String destDir, String destXmlFile) throws Exception {
      File srcFile = new File(TMPDIR +"/"+ destXmlFile);
      String destFile = destDir +"/"+ destXmlFile;
      System.out.println("sending to dav: "+ srcFile.getAbsolutePath());
       
      InputStream isr = new FileInputStream(srcFile);
      try { 
         davService.saveFile(destFile, isr);
         FileUtils.deleteQuietly(srcFile);
      } catch (UnsupportedEncodingException ex) {
         throw ex;
      } catch (Exception ex) {
         throw ex;
      } finally {
         isr.close();
      }
   }
   

   /**
    * @param destXml
    * @return
    * @throws Exception
    */
   private  MarcXmlWriter getWriter(String destXml) throws Exception {
      //System.out.println("Creating writer at: "+ TMPDIR +"/"+ destXml);
      OutputStream out = new FileOutputStream(new File(TMPDIR + "/" + destXml));

      MarcXmlWriter writer = null;
      writer = new MarcXmlWriter(out, "UTF8", true); //, createXml11);
      //writer.setIndent(doIndentXml);
      //writer.setCreateXml11(createXml11);
      setConverter(writer);
      if (normalize == true) {
         writer.setUnicodeNormalization(true);
      }

      return writer;
   }
   
   /**
    * @param writer
    * @throws Exception
    */
   private  void setConverter(MarcXmlWriter writer) throws Exception {

      if (null != convertEncoding) {
         CharConverter charconv = null;
         try {
            if (convertEncoding != null) {
               if (MARC_8_ENCODING.equals(convertEncoding)) {
                  charconv = new AnselToUnicode();
               } else if (ISO5426_ENCODING.equals(convertEncoding)) {
                  charconv = new Iso5426ToUnicode();
               } else if (ISO6937_ENCODING.equals(convertEncoding)) {
                  charconv = new Iso6937ToUnicode();
               } else {
                  throw new Exception("Unknown character set");
               }
               writer.setConverter(charconv);
            }
         } catch (javax.xml.parsers.FactoryConfigurationError e) {
            e.printStackTrace();
            throw new Exception(e);
         } catch (MarcException e) {
            e.printStackTrace();
            throw new Exception("There is a problem with character conversion: "
               + convertEncoding + " " + e);
         } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e);
         }
      }
   }
   
   /**
    * @param str
    * @return
    * @throws UnsupportedEncodingException
    */
   protected  InputStream stringToInputStream(String str) throws UnsupportedEncodingException {
      byte[] bytes = str.getBytes("UTF-8");
      return new ByteArrayInputStream(bytes);   
   } 
}
