package edu.cornell.library.integration.indexer.utilies;

import java.io.StringReader;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import edu.cornell.library.integration.ilcommons.service.DavService;

public class IndexingUtilities {
	
	public static String substitueInRecordURI(String recordURI, String query) {
		if( query == null )
			return null;			
		return query.replaceAll("\\$recordURI\\$", "<"+recordURI+">");		
	}

	public static String toString(SolrInputDocument doc) {
		String out ="SolrInputDocument[\n" ;
		for( String name : doc.getFieldNames()){
			SolrInputField f = doc.getField(name);
			out = out + "  " + name +": '" + f.toString() + "'\n";
		}
		return out + "]\n";						
	}
	
	/**
	 * For all newFields, add them to doc, taking into account
	 * fields that already exist in doc and merging the
	 * values of the new and existing fields. 
	 */
	public static void combineFields(SolrInputDocument doc,
			Map<? extends String, ? extends SolrInputField> newFields) {		
		for( String newFieldKey : newFields.keySet() ){
			SolrInputField newField = newFields.get(newFieldKey);

			if (newField.getValueCount() == 0) continue;
			
			if( doc.containsKey( newFieldKey )){
				SolrInputField existingField=doc.get(newFieldKey);
				mergeValuesForField(existingField, newField);
			}else{
				doc.put(newFieldKey, newField);				
			}
		}		
	}

	/**
	 * Call existingField.addValue() for all values form newField.
	 */
	public static void mergeValuesForField(SolrInputField existingField,
			SolrInputField newField) {
		for( Object value  : newField.getValues() ){
			existingField.addValue(value, 1.0f);
		}
	}
	

	


	//from http://stackoverflow.com/questions/139076/how-to-pretty-print-xml-from-java	
	public static String prettyFormat(String input, int indent) {
	    try {
	        Source xmlInput = new StreamSource(new StringReader(input));
	        StringWriter stringWriter = new StringWriter();
	        StreamResult xmlOutput = new StreamResult(stringWriter);
	        TransformerFactory transformerFactory = TransformerFactory.newInstance();
//        transformerFactory.setAttribute("indent-number", indent); //removed this line due to runtime exception
		    Transformer transformer = transformerFactory.newTransformer(); 
		    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		    transformer.transform(xmlInput, xmlOutput);
		    return xmlOutput.getWriter().toString();
	} catch (Exception e) {
	    throw new RuntimeException(e); // simple exception handling, please review it
	    }
	}
	
	public static String prettyFormat(String input) {
	    return prettyFormat(input, 2);
	}

    /**
     * Utility method for finding most recent files in a directory 
     * where the file names follow the pattern :
     *  
     *  directoryURL/fileNamePrefix-yyyy-MM-dd.txt
     *  
     *  Such as:
     *  
     *  http://example.com/files/bibs-2013-12-27.txt
     *  http://example.com/files/bibs-2013-12-28.txt
     *  http://example.com/files/bibs-2013-12-29.txt
     *  
     * @param davService 
     * @param directoryURL - directory to check for most recent files
     * @param fileNamePrefix - prefix for file names
     * @return name of most recent file, or null if none found. 
     *   The returned string will be the full URL to the file.
     * @throws Exception if there is a problem with the WEBDAV service. 
     */
    public static String findMostRecentFile(DavService davService , String directoryURL, String fileNamePrefix ) throws Exception{
       return findMostRecentFile(davService,directoryURL,fileNamePrefix,".txt");
    }

    
    /**
     * Utility method for finding most recent files in a directory 
     * where the file names follow the pattern :
     *  
     *  directoryURL/fileNamePrefix-yyyy-MM-dd.fileNamePostfix
     *  
     *  Such as:
     *  
     *  http://example.com/files/bibs-2013-12-27.nt.gz
     *  http://example.com/files/bibs-2013-12-28.nt.gz
     *  http://example.com/files/bibs-2013-12-29.nt.gz
     *  
     * @param davService 
     * @param directoryURL - directory to check for most recent files
     * @param fileNamePrefix - prefix for file names
     * @param fileNamePostfix - ending for file names
     * 
     * @return name of most recent file, or null if none found. 
     *   The returned string will be the full URL to the file.
     * @throws Exception if there is a problem with the WEBDAV service. 
     */
    public static String findMostRecentFile(
            DavService davService, 
            String directoryURL, 
            String fileNamePrefix, 
            String fileNamePostfix)throws Exception{
        
        if( ! directoryURL.endsWith("/"))
            directoryURL = directoryURL + "/";
        
        if( ! fileNamePostfix.startsWith("."))
            fileNamePostfix = "." + fileNamePostfix;
        
        Pattern p = Pattern.compile(fileNamePrefix + "-(....-..-..)" + fileNamePrefix);
        Date lastDate = new SimpleDateFormat("yyyy").parse("1900");
        String mostRecentFile = null;
                               
        List<String> biblists = davService.getFileList( directoryURL );   
        
        Iterator<String> i = biblists.iterator();            
        while (i.hasNext()) {
            String fileName = i.next();
            Matcher m = p.matcher(fileName);
            if (m.matches()) {
                Date thisDate = new SimpleDateFormat("yyyy-MM-dd").parse(m.group(1));
                if (thisDate.after(lastDate)) {
                    lastDate = thisDate;
                    mostRecentFile = fileName;
                }
            }
        }
        return directoryURL +  mostRecentFile;
        
    }
}
