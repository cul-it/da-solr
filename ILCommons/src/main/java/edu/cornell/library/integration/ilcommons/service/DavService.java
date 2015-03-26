package edu.cornell.library.integration.ilcommons.service;

 
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;


/**
 * @author jaf30
 *
 */
public interface DavService {   
    
	/**
	 *
	 * @param davUrl
	 * @param localPath
	 * Establishes an equivalence between a remote davUrl and a local file path. Some
	 * dav methods may use the local file path instead of the webdav interface when available.
	 */
	public void setDavUrlToLocalPathMapping( String davUrl, String localPath );
	
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
 * @throws Exception 
    */
   public List<String> getFileUrlList(String url) throws IOException, Exception;

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
    * Get the file at url from WEBDAV, save it to the local file 
    * outFile and return a File to outFile.
    */
   public File getFile(String url, String localOutFile) throws Exception;
   
   /**
    * @param url
    * @return
    * @throws Exception
    */
   public Path getNioPath(String url) throws Exception;
   
   /**
    * @param url
    * @param dataStream
    * @throws Exception
    */
   public void saveFile(String url, InputStream dataStream) throws Exception;
   
	/**
	 * @param url
	 * @param path
	 * @throws Exception
	 */
	public void saveNioPath(String url, Path path) throws Exception;
   
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

   
   /**
    * Attempt to make the directory on the WEBDAV server.
 * @throws IOException 
    */
   public void mkDir(String dir) throws IOException;

   /**
    * Attempt to make the directory on the WEBDAV server.
 * @throws IOException 
    */
   public void mkDirRecursive(String dir) throws IOException;
   
}
