package edu.cornell.library.integration.service;

import java.util.List;

import edu.cornell.library.integration.bo.BibData;
import edu.cornell.library.integration.bo.Location;

public interface CatalogService { 
   
   public List<Location> getAllLocation() throws Exception;
   
   public List<String> getRecentBibIds() throws Exception;
   
   public BibData getBibData(String bibid) throws Exception;
    
   
}
