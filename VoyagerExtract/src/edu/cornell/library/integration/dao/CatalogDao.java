package edu.cornell.library.integration.dao;

import java.util.List;
import java.util.Map;

import edu.cornell.library.integration.bo.BibData;
import edu.cornell.library.integration.bo.Location;


public interface CatalogDao {
   public int sanityCheck();
   
   public List<Location> getAllLocation() throws Exception;
   
   public List<String> getRecentBibIds() throws Exception;
   
   public BibData getBibData(String bibid) throws Exception;
   
}
