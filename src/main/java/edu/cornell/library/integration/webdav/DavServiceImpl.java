package edu.cornell.library.integration.webdav;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import com.googlecode.sardine.DavResource;
import com.googlecode.sardine.Sardine;
import com.googlecode.sardine.SardineFactory;
import com.googlecode.sardine.impl.SardineException;



class DavServiceImpl implements DavService {

    private String davUser;
    private String davPass;
    private String davBaseWebURL;
    private String localBaseFilePath;

    /**
     * @param davUser
     * @param davPass
     */
    DavServiceImpl(String davUser, String davPass) {
        this.davUser = davUser;
        this.davPass = davPass;
    }
    
    public void setDavUrlToLocalPathMapping( String davUrl, String localPath ){
    	davBaseWebURL = davUrl;
    	localBaseFilePath = localPath;
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
     * @see edu.cornell.library.integration.service.DavService#getResourceList(java.lang.String)
     */
    public List<DavResource> getResourceList(String url) throws IOException {
        try{
            Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
            return sardine.list(url);
        }catch (IOException e){
            throw new IOException( "Problem while trying to get file list from " + url ,e);
        }
    }

    /* (non-Javadoc)
     * @see edu.cornell.library.integration.service.DavService#getFileList(java.lang.String)
     */
    public List<String> getFileList(String url) throws IOException {
        try{
            List<String> filelist = new ArrayList<>();
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
            List<String> filelist = new ArrayList<>();
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
     * @see edu.cornell.library.integration.service.DavService#getFileAsInputStream(java.lang.String)
     */
    public InputStream getFileAsInputStream(String url) throws IOException {
    	
    	if (localBaseFilePath != null) {
    		String path = url.replace(davBaseWebURL, localBaseFilePath);
    		if ( ! path.equals(url)) {
    			File initialFile = new File(path);
    			System.out.println("Reading file from local filesystem: "+path);
    		    return new FileInputStream(initialFile);
    		}
    	}
    	
    	
        try{
            //System.out.println("Getting file: "+ url);
            Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
            InputStream istream = sardine.get(url);
            return istream;
        }catch(IOException e){
            throw new IOException("Problem while getting file as InputStream: " + url ,e );
        }
    }

    public File getFile(String url, String outFile) throws IOException {
        try{
            Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
            File file = new File(outFile); 
            try (   InputStream is  = sardine.get(url);
            		OutputStream os = new FileOutputStream(file) ) {
            	int len; 
            	byte buf[] = new byte[1024];
            	while ((len=is.read(buf))>0) {
            		os.write(buf,0,len);   
            	}
            }
            return file;
        } catch (SardineException e1) {
           	if (e1.getStatusCode() == 404)
        		return null;
			throw new IOException("Problem while getting file : " 
			        + url + " to local file " + outFile ,e1 );
        }catch(IOException e){
            throw new IOException("Problem while getting file : " 
                    + url + " to local file " + outFile ,e );
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
