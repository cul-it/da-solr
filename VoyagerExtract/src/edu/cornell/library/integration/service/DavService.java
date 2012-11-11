package edu.cornell.library.integration.service;

import java.io.InputStream;
import java.io.IOException;
import java.util.List;


public interface DavService {   
    
   public List<String>  getFileList(String url) throws IOException;
   public InputStream getFile(String url) throws Exception;
   public void saveFile(String fileName, InputStream dataStream) throws Exception;

   
}
