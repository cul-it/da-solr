package edu.cornell.library.integration;

import static edu.cornell.library.integration.utilities.IndexingUtilities.xml2SolrInputDocument;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;

public class GeneralTests {

	private static final Pattern uPlusHexPattern = Pattern.compile(".*[Uu]\\+\\p{XDigit}{4}.*");

	public GeneralTests() throws IOException, XMLStreamException {
		String[] testStrings =
			{"String Matches U+00C0",
			 "String Doesn't match U+HI!",
			 "String U+0034ABC should match too.",
			 "u+abcd should this one?"};
		for (String test : testStrings) {
			Boolean matches = uPlusHexPattern.matcher(test).matches();
			System.out.println(test + ((matches)?": matches.":": doesn't match."));
		}
		SolrInputDocument doc = new SolrInputDocument();
		doc.addField("id", 12);
		doc.addField("title","Test Document");
		doc.addField("boostedField", 87, 2);
		doc.addField("valHasBrackets", "[ test value ]");
		doc.addField("mv", 1);
		doc.addField("mv", "2");
		doc.addField("mv",3,2);
		doc.setDocumentBoost(0.5f);
		List<String> multival = new ArrayList<String>();
		multival.add("value 1, 2, 3");
		multival.add("value 4, 5, 6");
		doc.addField("multival", multival);
		doc.addField("quotedValue", "\"Here's a \"quoted\" value with \"quoted\" terms.\"");
		doc.addField("htmlTerm", "<a href=\"where's\">here's</a> a value with <b>dangerous</b> html tags. </field></doc>");
		System.out.println(ClientUtils.toXML(doc).replaceAll("</field>","$0\n"));
		SolrInputDocument doc2 = xml2SolrInputDocument(ClientUtils.toXML(doc));
		System.out.println(ClientUtils.toXML(doc2).replaceAll("</field>","$0\n"));
		if ( ! ClientUtils.toXML(doc).equals(ClientUtils.toXML(doc2)))
			System.out.println("Documents differ.");
		
		System.out.println(new SimpleDateFormat("EEEEE").format(new Date()));
	}

	public static void main(String[] args) {
		try {
			new GeneralTests();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (XMLStreamException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
