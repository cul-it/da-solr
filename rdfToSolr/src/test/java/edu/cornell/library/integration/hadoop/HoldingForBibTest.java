package edu.cornell.library.integration.hadoop;

import java.util.Collection;
import java.util.Set;

import static org.junit.Assert.*;
import org.junit.Test;

import edu.cornell.library.integration.hadoop.helper.HoldingForBib;


public class HoldingForBibTest {

	@Test
	public void holdingTest() throws Exception{
		
		HoldingForBib service = new HoldingForBib("http://jaf30-dev.library.cornell.edu:8080/DataIndexer/showTriplesLocation.do");
		Collection<String> holdingUrls = service.getHoldingUrlsForBibURI( "http://da-rdf.library.cornell.edu/individual/b3309" );
		assertNotNull(holdingUrls);
		assertTrue(holdingUrls.size() == 1 );
		assertEquals("http://culdata.library.cornell.edu/data/voyager/mfhd/mfhd.nt.full/mfhd.1.nt.gz", holdingUrls.iterator().next());
		
	}
	
	@Test
	public void uriTest(){		
		assertEquals("1234", HoldingForBib.bibIdForURI( "http://bogus.com/blk/b1234"));
	}

}