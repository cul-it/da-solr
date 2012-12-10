package edu.cornell.library.integration.hadoop;

import static junit.framework.Assert.*;

import java.io.IOException;
import java.util.List;

import org.junit.Test;


public class MarcToSolrUtilsTest {

	@Test
	public void getURLsTest() throws IOException{
		List<String> urls = MarcToSolrUtils.getLinksFromPage("http://culsearchdev.library.cornell.edu/data/voyager/bib/bib.nt.full/");
		assertNotNull( urls );		
		assertTrue( urls.size() > 0 );
	}
}
