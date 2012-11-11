package edu.cornell.library.integration.dao;

import java.util.List;
import java.util.Map;

import edu.cornell.library.integration.bo.Location;


public interface CatalogDao {
   public int sanityCheck();
   
   public List<Location> getAllLocation() throws Exception;
}
