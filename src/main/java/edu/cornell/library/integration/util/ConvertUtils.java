package edu.cornell.library.integration.util; 

import static edu.cornell.library.integration.util.MarcToXmlConstants.LN;
import static edu.cornell.library.integration.util.MarcToXmlConstants.WEIRD_CHARACTERS;
import static edu.cornell.library.integration.util.MarcToXmlConstants.WEIRD_CHARACTERS_PATTERN;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
//      String NonleaderReplaced = "The character is replaced with space.\n";
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
    * @param str
    * @return
    * @throws UnsupportedEncodingException
    */
   public  static InputStream stringToInputStream(String str) {
      byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
      return new ByteArrayInputStream(bytes);   
   }
   

   protected final Log logger = LogFactory.getLog(getClass());
   
}
