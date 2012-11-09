package edu.cornell.library.integration.service;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

@ContextConfiguration(locations={"classpath:spring.xml"})
public class DavServiceTest extends AbstractJUnit4SpringContextTests {
   
   @Test
   public void testContext() {
      Assert.assertNotNull(applicationContext.getBean("davService"));
   }
   
   @Test
   public void testGetFileList() {
      DavService davService = (DavService) applicationContext.getBean("davService");
      String propName = "voyager_bib_mrc_daily";
      try {
         List<String> filelist = davService.getFileList("http://jaf30-dev.library.cornell.edu/data/voyager/bib/bib.mrc.daily");
         for (String s : filelist) {
            System.out.println(s);
         }
      } catch (Exception e) { 
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   }

}
