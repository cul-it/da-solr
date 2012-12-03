package edu.cornell.library.integration.dao;

import java.util.List;
import java.util.Map;

import edu.cornell.library.integration.bo.BibBlob;
import edu.cornell.library.integration.bo.BibData;
import edu.cornell.library.integration.bo.Location;


public interface CatalogDao {
   
   /**
    * @return
    */
   public int sanityCheck();
   
   /**
    * @return
    * @throws Exception
    */
   public List<Location> getAllLocation() throws Exception;
   
   /**
    * @return
    * @throws Exception
    */
   public List<String> getRecentBibIds(String dateString) throws Exception;
   
   /**
    * @return
    * @throws Exception
    */
   public int getRecentBibIdCount(String dateString) throws Exception;
   
   /**
    * @param bibid
    * @return
    * @throws Exception
    */
   public BibBlob getBibBlob(String bibid) throws Exception;
   
   /**
    * @param bibid
    * @return
    * @throws Exception
    */
   public List<BibData> getBibData(String bibid) throws Exception;
   
}
