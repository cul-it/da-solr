package edu.cornell.library.integration.util; 

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.transform.Result;
import javax.xml.transform.sax.SAXResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.xml.serialize.OutputFormat;
import org.apache.xml.serialize.XMLSerializer;
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

public class ConvertUtils {
   
   protected final Log logger = LogFactory.getLog(getClass());
   
   /** The range of non allowable characters in XML 1.0 (ASCII Control Characters) */
   private static final String WEIRD_CHARACTERS =
      "[\u0001-\u0008\u000b-\u000c\u000e-\u001f]";
   private static final Pattern WEIRD_CHARACTERS_PATTERN =
         Pattern.compile(WEIRD_CHARACTERS);
   public static final String LN = System.getProperty("line.separator");
   
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
   
   private  int splitSize = 10000;

   public ConvertUtils() {
      // TODO Auto-generated constructor stub
   }
   
   public void setNormalize(boolean normalize) {
      this.normalize = normalize;
   }

   public int getSplitSize() {
      return splitSize;
   }

   public void setSplitSize(int splitSize) {
      this.splitSize = splitSize;
   } 
   
   public String getControlNumberOfLastReadRecord() {
      return controlNumberOfLastReadRecord;
   }

    

  
   public String convertMrcToXml(String mrc) throws Exception {
      String xml = new String();
      MarcXmlWriter writer = null;
      boolean hasInvalidChars;
      /** record counter */
      int counter = 0;

      /** the previous percent value */
      int prevPercent = 0;

      /** the percent of imported records in the size of file */
      int percent;
      
      Record record = null;

      long fileSize = mrc.length();

      InputStream is = stringToInputStream(mrc);
      //MarcReader reader = new MarcStreamReader(is);
      MarcPermissiveStreamReader reader = null;
      boolean permissive      = true;
      boolean convertToUtf8   = true;
      reader = new MarcPermissiveStreamReader(is, permissive, convertToUtf8);     
       
      OutputFormat format = new OutputFormat("xml", "UTF-8", false);
      StringWriter sw = new StringWriter();
      XMLSerializer serializer = new XMLSerializer(sw, format);
      Result result = new SAXResult(serializer.asContentHandler());
      writer = new MarcXmlWriter(result);
      setConverter(writer);
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
   
            if ((0 == counter % 100)) {
               System.out.print('.');
               if (reader.hasNext()) {
                  try {
                     if (is != null && is.available() != 0) {
                        percent = (int) ((fileSize - is.available()) * 100 / fileSize);
                        if ((0 == percent % 10) && percent != prevPercent) {
                           System.out.println(" (" + percent + "%)");
                           prevPercent = percent;
                        }
                     }
                  } catch (IOException e) {
                     e.printStackTrace();
                  }
               }
               System.gc();
            }
         }
         
      } finally {
         if (writer != null) {
            try {
               writer.close();
            } catch (Exception ex) {}
            
         }
         try { 
            is.close();
         } catch (IOException e) {
            e.printStackTrace();
         } 
      }
      System.out.println("record count: "+ counter);
      xml = sw.toString(); 
      return xml;
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
   
   private  MarcXmlWriter getWriter() throws Exception {
      MarcXmlWriter writer = null;
      OutputFormat format = new OutputFormat("xml", "UTF-8", true);
      StringWriter sw = new StringWriter();
      XMLSerializer serializer = new XMLSerializer(sw, format);
      Result result = new SAXResult(serializer.asContentHandler());
      writer = new MarcXmlWriter(result);
      setConverter(writer);
      if (normalize == true) {
         writer.setUnicodeNormalization(true);
      }

      return writer;
   }
   
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
