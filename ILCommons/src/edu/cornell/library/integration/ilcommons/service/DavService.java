package edu.cornell.library.integration.ilcommons.service;

 
import java.io.InputStream;
import java.io.IOException;
import java.util.List;


/**
 * @author jaf30
 *
 */
public interface DavService {   
    
   /**
    * @param url
    * @return list of filenames
    * @throws IOException
    */
   public List<String>  getFileList(String url) throws IOException;
   
   /**
    * @param url
    * @return list of file urls
    * @throws IOException
    */
   public List<String> getFileUrlList(String url) throws IOException;
   
   /**
    *  @param url
    * @return list of data directories
    * @throws IOException
    */
   //public List<String> getDirectories(String url) throws IOException;
   
   /**
    * @param url
    * @return a File as a String
    * @throws Exception
    */
   public String getFileAsString(String url) throws Exception;
   
   /**
    * @param url
    * @return an InputStream
    * @throws Exception
    */
   public InputStream getFileAsInputStream(String url) throws Exception;
   
   /**
    * @param url
    * @param dataStream
    * @throws Exception
    */
   public void saveFile(String url, InputStream dataStream) throws Exception;
   
   /**
    * @param url
    * @param bytes
    * @throws Exception
    */
   public void saveBytesToFile(String url, byte[] bytes ) throws Exception;
   
   /**
    * @param url
    * @throws Exception
    */
   public void deleteFile(String url) throws Exception;
   
   /**
    * @param srcUrl
    * @param destUrl
    * @throws Exception
    */
   public void moveFile(String srcUrl, String destUrl) throws Exception;

   
}
