package edu.cornell.library.integration.hadoop;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.LinkedList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class MarcToSolrUtils {
	
	public static void saveLinksToFile( String url, String fileName) throws IOException{		
		List<String>links = getLinksFromPage(url);
		FileOutputStream os = new FileOutputStream(fileName); 
		OutputStreamWriter out = new OutputStreamWriter(os,"UTF-8");
		for( String s : links){
			out.write(s );
			out.write('\n');
		}
	}
	
	public static List<String> getLinksFromPage(String url) throws  IOException{
		List<String> links = new LinkedList<String>();
	    Document document = Jsoup.connect(url ).get();
	    Elements linkTags = document.getElementsByTag("a");
	    for (Element link : linkTags) {
	      links.add(link.attr("href"));		      
	    }	     		
		return links;
	}

}
