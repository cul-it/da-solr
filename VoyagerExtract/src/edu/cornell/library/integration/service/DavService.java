package edu.cornell.library.integration.service;

import java.io.IOException;
import java.util.List;

public interface DavService {   
    
   public List<String>  getFileList(String pathProp) throws IOException;
}
