package edu.cornell.library.integration.hadoop.helper;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.LinkedList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class MarcToSolrUtils {
	
	public static void saveLinksToFile( String urlOfIndex, String fileName) throws IOException{		
		List<String>links = getLinksFromPage(urlOfIndex);
		FileOutputStream os = new FileOutputStream(fileName); 
		OutputStreamWriter out = new OutputStreamWriter(os,"UTF-8");
		for( String s : links){
			out.write(s );
			out.write('\n');
		}
	}
	
	public static List<String> getLinksFromPage(String urlOfIndex) throws  IOException{
		List<String> links = new LinkedList<String>();
	    Document document = Jsoup.connect(urlOfIndex ).get();
	    Elements linkTags = document.getElementsByTag("a");
	    for (Element link : linkTags) {
	      links.add(link.attr("href"));		      
	    }	     		
		return links;
	}

	public static String writeModelToNTString(Model m){
		if( m == null ) return "";		
		StringWriter writer = new StringWriter();
		m.write( writer , "N-TRIPLES");				
		return writer.toString();
	}	
	
	public static Model readNTStringToModel( String str){
		Model m = ModelFactory.createDefaultModel();
		if( str == null ) 
			return m;
		StringReader reader = new StringReader(str);
		m.read(reader, null, "N-TRIPLES");
		return m;
	}
}
