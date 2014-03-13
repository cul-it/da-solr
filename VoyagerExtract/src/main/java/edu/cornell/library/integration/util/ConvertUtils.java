package edu.cornell.library.integration.util; 

import static edu.cornell.library.integration.util.MarcToXmlConstants.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.marc4j.MarcException;
import org.marc4j.MarcPermissiveStreamReader;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.DataField;
import org.marc4j.marc.Record;
import org.marc4j.marc.Subfield;
import org.marc4j.marc.VariableField;
import org.marc4j.marc.impl.LeaderImpl;
import org.marc4j.marc.impl.SubfieldImpl;

/**
 * This class converts MARC21 to MARC XML.
 *
 */
public class ConvertUtils {  
   
  
    /**
     * This converts a single string to a MARC Record object. 
     * 
     * This method was written for use in unit tests and may 
     * work in unexpected ways.   
     * 
     * It should be passed a String containing a single MARC
     * record.        
     */
   public static Record getMarcRecord(String mrc){
       return getMarcRecord(mrc,null);
   }
   
   /**
    * This converts a single string to a MARC Record object. 
    * 
    * This method was written for use in unit tests and may 
    * work in unexpected ways.   
    * 
    * It should be passed a String containing a single MARC
    * record.    
    * 
    * convertEncoding may be null. If it is set to MARC_8_ENCODING
    * the the Char Coding Scheme in the leader of the records will be set to 'a'. 
    */
   public static  Record getMarcRecord(String mrc, String convertEncoding ) {
	  
	   Record record = null;
	   MarcPermissiveStreamReader reader = null;
	   boolean permissive      = true;
	   boolean convertToUtf8   = true;
	   InputStream is = null;
	   try {
		  is = stringToInputStream(mrc);
		  reader = new MarcPermissiveStreamReader(is, permissive, convertToUtf8);
	      while (reader.hasNext()) {
	         try {
	            record = reader.next();
	         } catch (MarcException me) {
	            System.out.println("MarcException reading record: " + me.getMessage());
	            continue;
	         } catch (Exception e) {
	            e.printStackTrace();
	            continue;
	         }
	         String controlNumberOfLastReadRecord = record.getControlNumber();
	         if (MARC_8_ENCODING.equals( convertEncoding )) {
	            record.getLeader().setCharCodingScheme('a');
	         }
	             	         	        
	         boolean hasInvalidChars = dealWithBadCharacters(record );
	         if( hasInvalidChars )
	             throw new Error( "Could not convert record because it has bad characters.");

	      } 
	   } catch (UnsupportedEncodingException e) {
		  // TODO Auto-generated catch block
		  e.printStackTrace();
	   } finally {
	          
	      try { 
	         is.close();
	      } catch (IOException e) {
	         e.printStackTrace();
	      } 
	   }
	   return record;
   }
   
   
   
   
   /**
    * Replace bad characters in a MARC record.
    * 
    * @return false if everything went fine,
    * true if if there are still bad characters after the attempted replacement. 
    */
    public static boolean dealWithBadCharacters(Record record ) {
        String recordString = record.toString();

        Matcher matcher = WEIRD_CHARACTERS_PATTERN.matcher(recordString);
        if (!matcher.find())
            return false;

        List<Integer> invalidCharsIndex = new ArrayList<Integer>();
        do {
            invalidCharsIndex.add(matcher.start());
        } while (matcher.find());

        StringBuffer badCharLocator = new StringBuffer();
        List<String> invalidChars = new ArrayList<String>();
        for (Integer i : invalidCharsIndex) {
            RecordLine line = new RecordLine(recordString, i);

            badCharLocator.append(line.getErrorLocation()).append(LN);
            invalidChars.add(line.getInvalidChar() + " ("
                    + line.getInvalidCharHexa() + ")" + " position: " + i);
            String invalidCharacter = (line.getInvalidChar() + " ("
                    + line.getInvalidCharHexa() + ")" + "position: " + i);
            String badCharacterLocator = (line.getErrorLocation() + LN);

            modifyRecord(record, line, invalidCharacter, badCharacterLocator);

        }
        
        //check to make sure we dealt with all the weird characters
        matcher = WEIRD_CHARACTERS_PATTERN.matcher(record.toString());
        return matcher.find();
   }
   

   /**
    * @param record
    * @param line
    * @param invalidCharacter
    * @param badCharacterLocator
    */
   private static void modifyRecord(Record record, RecordLine line,
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
//         DataField fd = (DataField) record.getVariableField(tag);
         List<VariableField> fds = record.getVariableFields(tag);
         for (VariableField fdv: fds) {
        	 DataField fd = (DataField) fdv;
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
       
   }
  

   /**
    * This converts a string to a MARC Records
    * and  returns their IDs
    * 
    * This method was written for use in unit tests and may 
    * work in unexpected ways.   
    * 
    * It should be passed a String containing a MARC
    * records.    
    * 
    * convertEncoding may be null. If it is set to MARC_8_ENCODING
    * the the Char Coding Scheme in the leader of the records will be set to 'a'. 
    */
    public static List<String> getBibIdFromMarc(String mrc, String convertEncoding) {
        
        boolean hasInvalidChars;
        
        List<String> f001list = new ArrayList<String>();
        Record record = null;
        MarcPermissiveStreamReader reader = null;
        boolean permissive      = true;
        boolean convertToUtf8   = true;
        InputStream is = null;
        
        try {
           is = ConvertUtils.stringToInputStream(mrc);
           reader = new MarcPermissiveStreamReader(is, permissive, convertToUtf8);
           while (reader.hasNext()) {
              try {
                 record = reader.next();
                  
              } catch (MarcException me) {
                 System.out.println("MarcException reading record" + me.getMessage());
                 continue;
              } catch (Exception e) {
                 e.printStackTrace();
                 continue;
              }
              
              String controlNumberOfLastReadRecord = record.getControlNumber();
              
              if (MARC_8_ENCODING.equals( convertEncoding )) {
                 record.getLeader().setCharCodingScheme('a');
              }
                  
              hasInvalidChars =  ConvertUtils.dealWithBadCharacters(record);
              if( hasInvalidChars )
                  System.out.println("Encountered invalid characters");
              
              ControlField f001 = (ControlField) record.getVariableField("001");
              if (f001 != null) {
                 f001list.add(f001.getData().toString());
              } 
           } 
        } catch (UnsupportedEncodingException e) {
           // TODO Auto-generated catch block
           e.printStackTrace();
        } finally {
               
           try { 
              is.close();
           } catch (IOException e) {
              e.printStackTrace();
           } 
        }
        return f001list;
    }   
   
   /**
    * @param str
    * @return
    * @throws UnsupportedEncodingException
    */
   public  static InputStream stringToInputStream(String str) throws UnsupportedEncodingException {
      byte[] bytes = str.getBytes("UTF-8");
      return new ByteArrayInputStream(bytes);   
   }
   

   protected final Log logger = LogFactory.getLog(getClass());
   
}
