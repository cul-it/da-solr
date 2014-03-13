package edu.cornell.library.integration;


import static edu.cornell.library.integration.util.MarcToXmlConstants.*;

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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
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
import org.marc4j.marc.Record;

import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.util.ConvertUtils;



/**
 * Object for converting large files of MARC21 to MARC XML with
 * control over splitting the output up over a number of files.
 * 
 * If you need to simply convert a single Record see ConvertUtils. 
 * 
 */
public class MrcToXmlConverter {
        
    
    private  String controlNumberOfLastReadRecord = null;
    
    private String convertEncoding = null;
    
    /** perform Unicode normalization */
    private  boolean normalize = true;
    
    /** split output file into this many records. Set to 0 for no splits. */
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

    /** local directory to use for temporary files. */
    private String tmpDir = null;    
    
    public MrcToXmlConverter() { }
    
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
       return this.splitSize;
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
       return this.convertEncoding;
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
       return this.sequence_prefix;
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
       return this.itemId;
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
       return this.srcType;
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
       return this.extractType;
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
       return this.ts;
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
       return this.destDir;
    }

    /**
     * @param destDir the destDir to set
     */
    public void setDestDir(String destDir) {
       this.destDir = destDir;
    }

    public String getControlNumberOfLastReadRecord() {
       return this.controlNumberOfLastReadRecord;
    }
    
    public void setControlNumberOfLastReadRecord(String controlNumberOfLastReadRecord) {
        this.controlNumberOfLastReadRecord = controlNumberOfLastReadRecord;
    }
    
    /**
     * @param mrc
     * @param davService
     * @return
     * @throws Exception
     */
    public List<String> convertMrcToXml(DavService davService, String srcDir, String srcFile) throws Exception {
        
       MarcXmlWriter writer = null;
       boolean hasInvalidChars;
       /** record counter */
       int counter = 0;
       int total = 0;
       int batch = 0;      
       Record record = null;
       String destXmlFile = new String();
       String tmpFilePath = getTmpDir() + "/"+ srcFile;
       File f = davService.getFile(srcDir +"/"+ srcFile, tmpFilePath);
       FileInputStream is = new FileInputStream(f);
        
       MarcPermissiveStreamReader reader = null;
       boolean permissive      = true;
       boolean convertToUtf8   = true;
       List<String> f001list = new ArrayList<String>();
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
                 logger.error("Exception reading record", e);                 
                continue;
             }
             counter++; 
             total++;
             String controlNum = record.getControlNumber();
             setControlNumberOfLastReadRecord(controlNum);
             if (MARC_8_ENCODING.equals(getConvertEncoding())) {
                record.getLeader().setCharCodingScheme('a');
             }
             
             ControlField f001 = (ControlField) record.getVariableField("001");
             if (f001 != null) {
                f001list.add(f001.getData().toString());
             }  
             
             hasInvalidChars = ConvertUtils.dealWithBadCharacters(record);             
    
             if (!hasInvalidChars) {
                writer.write(record);
             }else{
                 System.out.println("Skipping record due to invalid characters" );
                 System.out.println(String.format( "ControlNumber: %s Record in "
                         + "total: %d Record in file: %d", controlNum, total, counter));
             }
             
             // check to see if we need to write out a batch of records
             if (getSplitSize() > 0 && counter >= getSplitSize()) {               
                
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
       
       System.out.println("\nTotal record count: "+ total);          
       return f001list;
        
    }
    
    // this method figures out what the output file name should be
    private String getOutputFileName(int batch) {
       StringBuffer sb = new StringBuffer();
       String sequence = new String();
       
       if (StringUtils.equals(getExtractType(), "single") ) {
          sb.append(getSrcType() +"."+ getTs() +"."+ getItemId() +".xml");
       } else if (StringUtils.equals(getExtractType(), "updates")) {
          sb.append(getSrcType() +".update."+ getTs() +"."+ getItemId() +".xml");   
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
     * @param destXml
     * @return
     * @throws Exception
     */
    private  MarcXmlWriter getWriter(String destXml) throws Exception {

       OutputStream out = new FileOutputStream(new File(getTmpDir()+ "/" + destXml));

       MarcXmlWriter writer = null;
       writer = new MarcXmlWriter(out, "UTF8", true); //, createXml11);
       //writer.setIndent(doIndentXml);
       //writer.setCreateXml11(createXml11);
       
       writer.setConverter( setConverter( getConvertEncoding() ));
       
       if (normalize == true) {
          writer.setUnicodeNormalization(true);
       }

       return writer;
    }
    
    /**
     * @param davService
     * @param destDir
     * @param destXmlFile
     * @throws Exception
     */
    private void moveXmlToDav(DavService davService, String destDir, String destXmlFile) throws Exception {
       File srcFile = new File(getTmpDir() +"/"+ destXmlFile);
       String destFile = destDir +"/"+ destXmlFile;
          
       InputStream isr = null;
       try {
          isr = new FileInputStream(srcFile);
          davService.saveFile(destFile, isr);
          System.out.println("Saved to webdav: "+ destFile );
          FileUtils.deleteQuietly(srcFile);
       } catch (Exception ex) {
          throw new Exception("Could not save from temp file " + srcFile + " to WEBDAV " + destFile, ex);
       } finally {
          IOUtils.closeQuietly( isr );
       }
    }
    

    /**
     * Get a CharConverter for use with the Marc converter for a given
     * encoding name.
     * Returns null if convertEncoding is null.
     * Throws an exception of there are any problems. 
     */
    private static  CharConverter setConverter( String convertEncoding ) throws Exception {
   
        if (convertEncoding == null) 
            return null;
        
        CharConverter charconv = null;
        try {
   
            if (MARC_8_ENCODING.equals(convertEncoding)) {
               charconv = new AnselToUnicode();
            } else if (ISO5426_ENCODING.equals(convertEncoding)) {
               charconv = new Iso5426ToUnicode();
            } else if (ISO6937_ENCODING.equals(convertEncoding)) {
               charconv = new Iso6937ToUnicode();
            } else {
               throw new Exception("Unknown character set");
            }
            return charconv;
        } catch (Throwable e) {             
            throw new Exception("Could not get the Chracter "
                + "Converter '"+convertEncoding + "'", e);
        }        
    }
    
    public String getTmpDir() {
        if( tmpDir == null )
            return TMPDIR;
        else if( tmpDir.endsWith( "/" ))
            return tmpDir.substring(0, tmpDir.length()-1 );
        else
            return tmpDir;
    }

    public void setTmpDir(String tmpDir) {
        this.tmpDir = tmpDir;
    }

    protected final Log logger = LogFactory.getLog(getClass());
}
