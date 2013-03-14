package edu.cornell.library.integration.hadoop.helper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.params.HttpClientParams;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;

/**
 * Use Data Index servcie to get the holding file URL for a BIB ID.
 * 
 * @author bdc34
 *
 */
public class HoldingForBib {
	String serviceURL;
	DefaultHttpClient httpclient ;
	String controler = "";
	
	public HoldingForBib(String url){
		serviceURL = url;
		httpclient = new DefaultHttpClient();
		
		BasicHttpParams params = new BasicHttpParams();
		HttpClientParams.setRedirecting(params, false);		
		httpclient.setParams( params );
	}

	/**
	 * Returns empty set if none found. 
	 * Should never returns null.
	 * @throws Exception on 500 or other problems.
	 */
	public List<String> getHoldingUrlsForBibURI(String uri) throws Exception {						
		HttpHead httphead= new HttpHead(serviceURL + "?bibid=" + bibIdForURI( uri ) );
		HttpResponse response = httpclient.execute(httphead);
		
		StatusLine stat = response.getStatusLine();
		if( stat.getStatusCode() >= 500 && stat.getStatusCode() < 600 )
			throw new Exception( "Error from server: " + stat.toString());
		
		if( stat.getStatusCode() == HttpStatus.SC_MOVED_PERMANENTLY 
		    || stat.getStatusCode() == HttpStatus.SC_MOVED_TEMPORARILY ){
			Header[] locations = response.getHeaders(HttpHeaders.LOCATION);
			if( locations == null )
				throw new Exception("No locations returned for " + stat.getStatusCode());
			ArrayList<String> locUrls= new ArrayList<String>( locations.length );
			for( Header header : locations){
				if( header.getValue() != null ){
					String value = header.getValue();
					if( value.contains("?"))
						locUrls.add( value.substring(0, value.indexOf("?")));
					else
						locUrls.add( value );
				}
			}
			return locUrls;
		}else{
			throw new Exception( "Expected to get the holding urls " +
					"in Location header of 30x but got " + stat.toString() +
					" from " + httphead.getURI());
		}						
	}
	
	public List<String> getHoldingUrlsForBibURI(Set<String> ids ) throws Exception {
		Set<String> urls = new HashSet<String>();
		for( String id : ids){
			urls.addAll( getHoldingUrlsForBibURI( id ) );
		}
		return new ArrayList<String>( urls );
	}
	
	/** Don't use this, we should be using opaque URIs */ 
	protected static  String bibIdForURI(String uri){
		if( uri == null )
			return null;
		int lastB = uri.lastIndexOf("b");
		if( lastB > 0 )
			return uri.substring(lastB + 1 );
		else
			throw new Error("There was no bib id in the URI");
	}
}
