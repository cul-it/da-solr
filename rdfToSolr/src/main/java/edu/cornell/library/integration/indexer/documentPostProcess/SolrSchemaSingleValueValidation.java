package edu.cornell.library.integration.indexer.documentPostProcess;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;

/**
 * Get the solr schema.xml and check number of vlaues for non-multi-value fields.
 *
 * Since this needs the Solr service's URL is isn't too useful for a RecordToDocument.
 */
public class SolrSchemaSingleValueValidation implements DocumentPostProcess {

	String solrUrl;
	String solrSchemaUrl;
	
	public SolrSchemaSingleValueValidation(String solrUrl) {
		super();
		this.solrUrl = solrUrl;
	}

	@Override
	public void p(String recordURI, RDFService mainStore,
			RDFService localStore, SolrInputDocument document, Connection voyager) throws Exception {
				
		solrSchemaUrl = solrUrl + "/admin/file/?contentType=text/xml;charset=utf-8&file=schema.xml";
		Document schemaXml = getSchemaXml();
		XPathFactory factory = XPathFactory.newInstance();
		
		for( String name : document.getFieldNames() ){
			SolrInputField field = document.getField(name);
			checkField( schemaXml, factory, field);
		}
	}

	private void checkField( Document schemaXml , XPathFactory factory, SolrInputField field ) throws Exception{
			    	    	    
		if( field != null && field.getValueCount() > 1 ){
			String xpathstr = "/schema/fields[1]/field[@name='"+field.getName()+"']/@multiValued";
		    
		    javax.xml.xpath.XPathExpression expr = factory.newXPath().compile(xpathstr);

		    Object result = expr.evaluate(schemaXml, XPathConstants.BOOLEAN);
		    Boolean multiValued = (Boolean)result;
		    if( multiValued != null &&  ! multiValued ){
		    	throw new Exception("Field " + field.getName() + " has more than one value but " +
		    			"is defined as not muluti valued in the solr schema.xml at " + solrSchemaUrl + 
		    			"\n" + field.toString());
		    }
		}
	}
	
	private Document getSchemaXml( ) throws ParserConfigurationException {
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		dBuilder = dbFactory.newDocumentBuilder();		
		try {
			URLConnection con = new URL( solrSchemaUrl ).openConnection();
			con.setRequestProperty("Accept-Charset", "UTF-8");			
			return dBuilder.parse( con.getInputStream() );
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		return null;
	}
}
