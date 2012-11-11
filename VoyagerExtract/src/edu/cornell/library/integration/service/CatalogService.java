package edu.cornell.library.integration.service;

import java.util.List;

import edu.cornell.library.integration.bo.Location;

public interface CatalogService { 
   
   public List<Location> getAllLocation() throws Exception;
    
   
}
