package edu.cornell.library.integration.util;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.charset.Charset;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;

import org.marc4j.marcxml.Converter;
import org.marc4j.marcxml.MarcXmlReader;
import org.xml.sax.InputSource;

public class ConvertUtils {

   public ConvertUtils() {
      // TODO Auto-generated constructor stub
   }
   
   public static String convertMrcToXml(String mrc) throws Exception {
      String xml = new String();
      char chr = mrc.charAt(mrc.length() - 1);
      //System.out.println("terminator: "+ (int) chr); 
      String reclenstr = mrc.substring(0,5); 
      //System.out.println("reclen: "+ reclenstr);
      //System.out.println("mrclen: "+ mrc.length());
 
      Writer writer = null;
      InputStream is = stringToInputStream(mrc);
      OutputStream ostream = null;
      try {
         ostream = new ByteArrayOutputStream();
         MarcXmlReader producer = new MarcXmlReader();
         org.marc4j.MarcReader reader = new org.marc4j.MarcReader(); 
         InputSource in = new InputSource(is);
         
         in.setEncoding("UTF-8");
         Source source = new SAXSource(producer, in);
          
         writer = new BufferedWriter(new OutputStreamWriter(ostream, "UTF-8"));
         Result result = new StreamResult(writer);
         Converter converter = new Converter();
         converter.convert(source, result);
         xml = new String(ostream.toString());
          
          
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
         throw e;
      } finally {
         ostream.close();
      } 
      return xml;

   }
   
   /**
    * @param str
    * @return
    * @throws UnsupportedEncodingException
    */
   protected static InputStream stringToInputStream(String str) throws UnsupportedEncodingException {
      byte[] bytes = str.getBytes("UTF-8");
      return new ByteArrayInputStream(bytes);   
   }
   
   /**
    * @param text
    * @return
    */
    public static boolean isAscii(String text) {
       Charset charset = Charset.forName("US-ASCII");  
       String checked = new String(text.getBytes(charset), charset);
       return checked.equals(text);
    }
    
    /**
     * @param text
     * @return
     */
     public static boolean isISO8859(String text) {
       
       Charset charset = Charset.forName("ISO-8859-1");      
       String checked = new String(text.getBytes(charset), charset);
       return checked.equals(text);
     }
     
     public static boolean isUTF8(String text) {
       
       Charset charset = Charset.forName("UTF-8");     
       String checked = new String(text.getBytes(charset), charset);
       return checked.equals(text);
      }
    
    /**
     * @param latin1
     * @return
     * @throws Exception
     */
    public static String fixEncoding(String latin1) throws Exception {
       try {
          byte[] bytes = latin1.getBytes("ISO-8859-1");
          if(!validUTF8(bytes))
             return latin1;
          return new String(bytes, "UTF-8");
       } catch (UnsupportedEncodingException e) {
          // Impossible, throw unchecked
          throw new IllegalStateException("No Latin1 or UTF-8: " + e.getMessage());
       }
       
    }
    
    /**
     * @param input
     * @return
     */
    public static boolean validUTF8(byte[] input) {
       int i = 0;
       // Check for BOM
       if(input.length >= 3 && (input[0] & 0xFF) == 0xEF
          && (input[1] & 0xFF) == 0xBB & (input[2] & 0xFF) == 0xBF) {
          i = 3;
       }
       
       int end;
       for(int j = input.length; i < j; ++i) {
          int octet = input[i];
          if((octet & 0x80) == 0) {
             continue; // ASCII
          }
          
          // Check for UTF-8 leading byte
          if((octet & 0xE0) == 0xC0) {
             end = i + 1;
          } else if((octet & 0xF0) == 0xE0) {
             end = i + 2;
          } else if((octet & 0xF8) == 0xF0) {
             end = i + 3;
          } else {
             // Java only supports BMP so 3 is max
             return false;
          }
          
          while(i < end) {
             i++;
             octet = input[i];
             if((octet & 0xC0) != 0x80) {
                // Not a valid trailing byte
                return false;
             }
          }
       }
       return true;
    }
   
   

   /**
    * @param args
    */
   public static void main(String[] args) {
      // TODO Auto-generated method stub

   }

}
