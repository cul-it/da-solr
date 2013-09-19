package edu.cornell.library.integration.ilcommons.service;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import com.googlecode.sardine.DavResource;
import com.googlecode.sardine.Sardine;
import com.googlecode.sardine.SardineFactory; 



public class DavServiceImpl implements DavService {
   
   private String davUser;
   private String davPass;

   public DavServiceImpl() {
      // TODO Auto-generated constructor stub
   }
   
   /**
    * @param davUser
    * @param davPass
    */
   public DavServiceImpl(String davUser, String davPass) {
      this.davUser = davUser;
      this.davPass = davPass;
   }

   /**
    * @return the davUser
    */
   public String getDavUser() {
      return this.davUser;
   }

   /**
    * @param davUser the davUser to set
    */
   public void setDavUser(String davUser) {
      this.davUser = davUser;
   }


   /**
    * @return the davPass
    */
   public String getDavPass() {
      return this.davPass;
   }

   /**
    * @param davPass the davPass to set
    */
   public void setDavPass(String davPass) {
      this.davPass = davPass;
   }

   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.DavService#getFileList(java.lang.String)
    */
   public List<String> getFileList(String url) throws IOException {
      List<String> filelist = new ArrayList<String>();
      Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
      List<DavResource> resources = sardine.list(url);
      for (DavResource res : resources) {
         if (! res.isDirectory()) {
            filelist.add(res.getName());
         }
      }
      return filelist;
   }
   
   public List<String> getFileUrlList(String url) throws IOException {
      List<String> filelist = new ArrayList<String>();
      Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
      String host = new String();
      String scheme = new String();
      try {
         URI baseuri = new URI(url);
         host = baseuri.getHost();
         scheme = baseuri.getScheme();
      } catch (URISyntaxException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      
      List<DavResource> resources = sardine.list(url);
      for (DavResource res : resources) {
         if (! res.isDirectory()) {
            URI uri = res.getHref();
            
            filelist.add(scheme + "://" + host +  uri.getPath());
         }
      }
      return filelist;
   }
   
   /**
    * @param url
    * @return
    * @throws IOException
    */
   // this does not seem to work
  /* public List<String> getDirectories(String url) throws IOException {
      List<String> dirList = new ArrayList<String>();
      Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
      // sardine does not allow passing the depth as an string (i.e. infinity)...set it to 99
      List<DavResource> resources = sardine.list(url, 5); 
      for (DavResource res : resources) {
         if (res.isDirectory()) {
            dirList.add(res.getName());
         }
      }
      return dirList;
   }*/

   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.DavService#getFileAsString(java.lang.String)
    */
   public String getFileAsString(String url) throws IOException {
      //System.out.println("Getting file: "+ url);
      Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
      InputStream istream = sardine.get(url);
      String str = convertStreamToString(istream);
      return str;
   }
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.DavService#getFileAsInputStream(java.lang.String)
    */
   public InputStream getFileAsInputStream(String url) throws IOException {
      //System.out.println("Getting file: "+ url);
      Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
      InputStream istream = (InputStream) sardine.get(url);
      return istream; 
   }
   
   public File getFile(String url, String outFile) throws IOException {
      Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
      InputStream is  = (InputStream) sardine.get(url);   
      File file = new File(outFile); 
      OutputStream os = new FileOutputStream(file); 
      int len; 
      byte buf[] = new byte[1024];
      while ((len=is.read(buf))>0) {
         os.write(buf,0,len);   
      }
      os.close(); 
      is.close();
      return file;
   }
   
   public Path getNioPath(String url) throws Exception {
	   Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
	   InputStream is  = (InputStream) sardine.get(url); 
	   CopyOption[] options = new CopyOption[]{
		  StandardCopyOption.REPLACE_EXISTING
       };
	   final Path path = Files.createTempFile("nio-temp", ".tmp");
	   path.toFile().deleteOnExit();
	   Files.copy(is, path, options);	     
	   return path;
   }

   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.DavService#saveFile(java.lang.String, java.io.InputStream)
    */
   public void saveFile(String url, InputStream dataStream) throws IOException {
      Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
      sardine.put(url, dataStream);
       
   }
   
	/* (non-Javadoc)
	 * @see edu.cornell.library.integration.ilcommons.service.DavService#saveNioPath(java.lang.String, java.nio.file.Path)
	 */
	public void saveNioPath(String url, Path path) throws Exception {
		Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
		File file = path.toFile();
		InputStream dataStream = null;
		try {
			dataStream = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		sardine.put(url, dataStream);
	}
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.DavService#saveBytesToFile(java.lang.String, byte[])
    */
   public void saveBytesToFile(String url, byte[] bytes) throws IOException {
      Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
      sardine.put(url, bytes);   
   }
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.ilcommons.service.DavService#deleteFile(java.lang.String)
    */
   public void deleteFile(String url) throws IOException {
      Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
      sardine.delete(url);       
   }
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.ilcommons.service.DavService#moveFile(java.lang.String, java.lang.String)
    */
   public void moveFile(String srcUrl, String destUrl) throws IOException {
      Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
      sardine.move(srcUrl, destUrl);    
   }
   
   protected String convertStreamToString(InputStream is)
         throws IOException {
     //
     // To convert the InputStream to String we use the
     // Reader.read(char[] buffer) method. We iterate until the
     // Reader return -1 which means there's no more data to
     // read. We use the StringWriter class to produce the string.
     //
     if (is != null) {
         Writer writer = new StringWriter();

         char[] buffer = new char[1024];
         try {
             Reader reader = new BufferedReader(
                     new InputStreamReader(is, "UTF-8"));
             int n;
             while ((n = reader.read(buffer)) != -1) {
                 writer.write(buffer, 0, n);
             }
         } finally {
             is.close();
         }
         return writer.toString();
     } else {       
         return "";
     }
 }
}
