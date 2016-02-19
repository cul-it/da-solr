package edu.cornell.library.integration.dao;

import java.util.List;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.marc4j.marc.Record;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

import edu.cornell.library.integration.bo.BibData;
import edu.cornell.library.integration.util.ConvertUtils;

@ContextConfiguration(locations={"classpath:test-spring.xml"})
public class CatalogDaoTestGetBibData extends AbstractJUnit4SpringContextTests {
   
   @Test
   @Ignore
   public void testContext() {
      Assert.assertNotNull(applicationContext.getBean("catalogDao"));
   }
   

   
  
   @Test
   public void testGetBibData() {
      System.out.println("testGetBibData");
      CatalogDao catalogDao = (CatalogDao) applicationContext.getBean("catalogDao");
      String bibid = "4579181";

      try {
    	  List<BibData>  bibDataList = catalogDao.getBibData(bibid);
          StringBuffer sb = new StringBuffer();
          for (BibData bibData : bibDataList) {
             sb.append(bibData.getRecord());
          }
          ConvertUtils convert = new ConvertUtils();
          @SuppressWarnings("static-access")
		Record record = convert.getMarcRecord(sb.toString());
          System.out.println(record.toString());
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

}
