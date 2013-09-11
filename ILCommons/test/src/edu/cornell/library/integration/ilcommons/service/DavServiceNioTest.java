package edu.cornell.library.integration.ilcommons.service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.Assert.*;

import org.junit.Ignore;
import org.junit.Test; 
 
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;

 
public class DavServiceNioTest {
    /*
	Path currentVoyagerBibList = Paths.get("http://culdatadev.library.cornell.edu/data/voyager/bib/unsuppressed/"+mostRecentBibFile);
	String line;
	BufferedReader reader = Files.newBufferedReader(currentVoyagerBibList, Charset.forName("US-ASCII"));
	while ((line = reader.readLine()) != null) {
		Integer bibid = Integer.valueOf(line);
		if (currentIndexBibList.contains(bibid)) {
			// bibid is on both lists.
			currentIndexBibList.remove(bibid);
		} else {
			bibsInVoyagerNotIndex.add(bibid);
		}
	}
	bibsInIndexNotVoyager.addAll(currentIndexBibList);
	currentIndexBibList.clear();
	reader.close();
    */
   
   @Test
   public void testGetNioPath() {
      System.out.println("\ntestGetNioPath\n");
      DavService davService = DavServiceFactory.getDavService();
      String line; 
      String url = "http://culdatadev.library.cornell.edu/data/test/test.txt";
      try {
        Path path = davService.getNioPath(url);
        BufferedReader reader = Files.newBufferedReader(path, Charset.forName("US-ASCII"));
     	while ((line = reader.readLine()) != null) {
     		System.out.println(line);
     	} 
      } catch (Exception e) { 
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   }
   
   
   @Test
	public void testSaveNioPath() {
		System.out.println("\ntestSaveNioPath\n");
		DavService davService = DavServiceFactory.getDavService();
		String url = "http://culdatadev.library.cornell.edu/data/test/test.txt";
		Charset charset = Charset.forName("US-ASCII");
		Path path = null;
		try {
			path = Files.createTempFile("nio-temp", ".tmp");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String testString = "This is a write test";
		try (BufferedWriter writer = Files.newBufferedWriter(path, charset)) {
			writer.write(testString, 0, testString.length());
		} catch (Exception ex) {
			System.err.format("IOException: %s%n", ex);
		}
		try {
			davService.saveNioPath(url, path);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Done.");

	}

}
