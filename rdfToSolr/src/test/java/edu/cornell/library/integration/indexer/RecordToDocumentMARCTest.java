package edu.cornell.library.integration.indexer;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocumentList;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RecordToDocumentMARCTest {
	SolrServer solr=null;
	String solrdir = "./junkjunk";
	
	@Rule
    public TemporaryFolder folder = new TemporaryFolder();
	
	@Before
	public void setup() throws SolrServerException, IOException{	
		solr = setupSolrIndex( folder.newFolder("recordToDocumentMARCTest_solr"));
		indexStandardTestRecords( solr );
		solr.commit();
	}
		
	@After
	public void tareDown(){
		//delete solrdir?
	}
	
	public SolrServer setupSolrIndex(File file){		 				
		CoreContainer.Initializer initializer = new CoreContainer.Initializer();
//		CoreContainer coreContainer = initializer.initialize();
//		EmbeddedSolrServer server = new EmbeddedSolrServer(coreContainer, "");		
		return null;
	}
	
	public void indexStandardTestRecords(SolrServer solr){
		fail("Not yet implemented");
	}
	
	public SolrDocumentList getDocsForQuery(String query){		
		fail("Not yet implemented");
		return null;
	}
	
	@Test
	public void test() {
		fail("Not yet implemented");
	}

}
