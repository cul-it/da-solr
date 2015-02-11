package edu.cornell.library.integration.ilcommons.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.googlecode.sardine.DavResource;
import com.googlecode.sardine.Sardine;
import com.googlecode.sardine.SardineFactory;
import com.googlecode.sardine.impl.SardineException;



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
        try{
            List<String> filelist = new ArrayList<String>();
            Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
            List<DavResource> resources = sardine.list(url);
            for (DavResource res : resources) {
                if (! res.isDirectory()) {
                    filelist.add(res.getName());
                }
            }
            java.util.Collections.sort(filelist);
            return filelist;
        }catch (IOException e){
            throw new IOException( "Problem while trying to get file list from " + url ,e);
        }
    }

    public List<String> getFileUrlList(String url) throws Exception {
        try{
            List<String> filelist = new ArrayList<String>();
            Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
            String host = new String();
            String scheme = new String();

            URI baseuri = new URI(url);
            host = baseuri.getHost();
            scheme = baseuri.getScheme();

            List<DavResource> resources = sardine.list(url);
            for (DavResource res : resources) {
                if (! res.isDirectory()) {
                    URI uri = res.getHref();

                    filelist.add(scheme + "://" + host +  uri.getPath());
                }
            }
            java.util.Collections.sort(filelist);
            return filelist;
        }catch (IOException e){
            throw new Exception( "Problem while trying to get file list from " + url ,e);
        }
    }


    /* (non-Javadoc)
     * @see edu.cornell.library.integration.service.DavService#getFileAsString(java.lang.String)
     */
    public String getFileAsString(String url) throws IOException {
        try{
            Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
            InputStream istream = sardine.get(url);      
            return IOUtils.toString(istream, "UTF-8");
        }catch(IOException e){
            throw new IOException("Problem while getting file as a String: " + url ,e );
        }
    }

    /* (non-Javadoc)
     * @see edu.cornell.library.integration.service.DavService#getFileAsInputStream(java.lang.String)
     */
    public InputStream getFileAsInputStream(String url) throws IOException {
        try{
            //System.out.println("Getting file: "+ url);
            Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
            InputStream istream = (InputStream) sardine.get(url);
            return istream;
        }catch(IOException e){
            throw new IOException("Problem while getting file as InputStream: " + url ,e );
        }
    }

    public File getFile(String url, String outFile) throws IOException {
        try{
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
        } catch (SardineException e1) {
        	if (e1.getMessage().contains("404 Not Found"))
        		return null;
        	else
                throw new IOException("Problem while getting file : " 
                        + url + " to local file " + outFile ,e1 );
        }catch(IOException e){
            throw new IOException("Problem while getting file : " 
                    + url + " to local file " + outFile ,e );
        }
    }

    public Path getNioPath(String url) throws Exception {
        try{
            Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
            InputStream is  = (InputStream) sardine.get(url); 
            CopyOption[] options = new CopyOption[]{
                    StandardCopyOption.REPLACE_EXISTING
            };
            final Path path = Files.createTempFile("nio-temp", ".tmp");
            path.toFile().deleteOnExit();
            Files.copy(is, path, options);	     
            return path;
        }catch(Exception e){
            throw new Exception("Problem getting file as NIO Path: " + url , e);
        }
    }

    /* (non-Javadoc)
     * @see edu.cornell.library.integration.service.DavService#saveFile(java.lang.String, java.io.InputStream)
     */
    public void saveFile(String url, InputStream dataStream) throws IOException {
        try{
            Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
            sardine.put(url, dataStream);
        }catch(IOException e){       
            throw new IOException("Problem while saving file to " + url ,e );       
        }

    }

    /* (non-Javadoc)
     * @see edu.cornell.library.integration.ilcommons.service.DavService#saveNioPath(java.lang.String, java.nio.file.Path)
     */
    public void saveNioPath(String url, Path path) throws Exception {
        try{
            Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
            File file = path.toFile();
            InputStream dataStream = null;    		
            dataStream = new FileInputStream(file);    		
            sardine.put(url, dataStream);
        }catch(IOException e){       
            throw new IOException("Problem while saving NIO Path to " + url ,e );       
        }
    }

    /* (non-Javadoc)
     * @see edu.cornell.library.integration.service.DavService#saveBytesToFile(java.lang.String, byte[])
     */
    public void saveBytesToFile(String url, byte[] bytes) throws IOException {
        try{
            Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
            sardine.put(url, bytes);   
        }catch(IOException e){
            throw new IOException("Problem while saving to file " + url, e);
        }
    }

    /* (non-Javadoc)
     * @see edu.cornell.library.integration.ilcommons.service.DavService#deleteFile(java.lang.String)
     */
    public void deleteFile(String url) throws IOException {
        try{
            Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
            sardine.delete(url);
        }catch(IOException e){
            throw new IOException("Problem deleting " + url, e);
        }
    }

    /* (non-Javadoc)
     * @see edu.cornell.library.integration.ilcommons.service.DavService#moveFile(java.lang.String, java.lang.String)
     */
    public void moveFile(String srcUrl, String destUrl) throws IOException {
        try{
            Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
            sardine.move(srcUrl, destUrl);
        }catch(IOException e){
            throw new IOException("Problem moving file from " + srcUrl + " to " + destUrl, e);
        }
    }

    @Override
    public void mkDir(String dir) throws IOException {
        try{
            Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
            sardine.createDirectory( dir );
        }catch(SardineException se){
            //oddly this seems to be an exception that gets thrown even when this works.
            if( se.getStatusCode() != 301 ){
                throw new IOException("Problem creating directory " + dir , se);
            }
        }catch(IOException e){
            throw new IOException("Problem creating directory " + dir , e);
        }   
    }    


    @Override
    public void mkDirRecursive(String dir) throws IOException {
        try{
            Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
            if (sardine.exists(dir)) {
            	return;
            }
            int lastSlash = dir.lastIndexOf('/');
        	if (lastSlash == -1) {
            	throw new IOException("Problem creating directory " +dir+". Invalid path?");
        	}
            String parentDir = dir.substring(0, lastSlash);
            mkDirRecursive( parentDir );
            sardine.createDirectory( dir );
        }catch(SardineException se){
            //oddly this seems to be an exception that gets thrown even when this works.
            if( se.getStatusCode() != 301 ){
                throw new IOException("Problem creating directory " + dir , se);
            }
        }catch(IOException e){
            throw new IOException("Problem creating directory " + dir , e);
        }   
    }  
    
}
