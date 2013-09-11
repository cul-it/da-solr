package edu.cornell.library.integration.indexer.updates;

import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.indexer.utilies.IndexRecordListComparison;

public class IdentifyDeletedRecords {
	
	private final String davUrl = "http://culdata.library.cornell.edu/data";

	DavService davService;
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String coreUrl = "http://fbw4-dev.library.cornell.edu:8080/solr/test";
		if (args.length >= 1)
			coreUrl = args[0];
		if (args.length >= 3) {
			DavService davService = DavServiceFactory.getDavService();
			try {
				Path currentVoyagerBibList = davService.getNioPath(args[1]);
				Path currentVoyagerMfhdList = davService.getNioPath(args[2]);
				new IdentifyDeletedRecords(coreUrl,currentVoyagerBibList,currentVoyagerMfhdList);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else 
			new IdentifyDeletedRecords(coreUrl);
	}
	
	public IdentifyDeletedRecords(String coreUrl, Path currentVoyagerBibList, Path currentVoyagerMfhdList) {

		davService = DavServiceFactory.getDavService();
		System.out.println("Comparing to contents of index at: " + coreUrl);

		IndexRecordListComparison c = new IndexRecordListComparison();
		c.compare(coreUrl, currentVoyagerBibList, currentVoyagerMfhdList);
		
		produceReport(c);

	}

	public IdentifyDeletedRecords(String coreUrl) {
		davService = DavServiceFactory.getDavService();
		Path currentVoyagerBibList = null;
		Path currentVoyagerMfhdList = null;
		try {
			List<String> biblists = davService.getFileList(davUrl+"/voyager/bib/unsuppressed");
			Pattern p = Pattern.compile("unsuppressedBibId-(....-..-..).txt"); 
			Iterator<String> i = biblists.iterator();
			Date lastDate = new SimpleDateFormat("yyyy").parse("1950");
			String mostRecentBibFile = null;
			while (i.hasNext()) {
				String fileName = i.next();
				Matcher m = p.matcher(fileName);
				if (m.matches()) {
					Date thisDate = new SimpleDateFormat("yyyy-mm-dd").parse(m.group(1));
					if (thisDate.after(lastDate)) {
						lastDate = thisDate;
						mostRecentBibFile = fileName;
					}
				}
			}
			if (mostRecentBibFile != null) {
				System.out.println("Most recent bib file identified as: "+davUrl+"/voyager/bib/unsuppressed/"+mostRecentBibFile);
				currentVoyagerBibList = davService.getNioPath(davUrl+"/voyager/bib/unsuppressed/"+mostRecentBibFile);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			List<String> mfhdlists = davService.getFileList(davUrl+"/voyager/mfhd/unsuppressed");
			Pattern p = Pattern.compile("unsuppressedMfhdId-(....-..-..).txt"); 
			Iterator<String> i = mfhdlists.iterator();
			Date lastDate = new SimpleDateFormat("yyyy").parse("1950");
			String mostRecentMfhdFile = null;
			while (i.hasNext()) {
				String fileName = i.next();
				Matcher m = p.matcher(fileName);
				if (m.matches()) {
					Date thisDate = new SimpleDateFormat("yyyy-mm-dd").parse(m.group(1));
					if (thisDate.after(lastDate)) {
						lastDate = thisDate;
						mostRecentMfhdFile = fileName;
					}
				}
			}
			if (mostRecentMfhdFile != null) {
				System.out.println("Most recent mfhd file identified as: "+davUrl+"/voyager/mfhd/unsuppressed/"+mostRecentMfhdFile);
				currentVoyagerMfhdList = davService.getNioPath(davUrl+"/voyager/mfhd/unsuppressed/"+mostRecentMfhdFile);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if ((currentVoyagerBibList != null) && (currentVoyagerMfhdList != null)) {
			System.out.println("Comparing to contents of index at: " + coreUrl);
			
			IndexRecordListComparison c = new IndexRecordListComparison();
			c.compare(coreUrl, currentVoyagerBibList, currentVoyagerMfhdList);
			
			produceReport(c);
		}
 	}

	// Based on the IndexRecordListComparison, bib records to be updated and deleted are printed to STDOUT
	// and also written to report files on the webdav server in the /updates/bib.deletes and /updates/bibupdates
	// folders. The report files have post-pended dates in their file names.
	private void produceReport( IndexRecordListComparison c ) {

		String currentDate = new SimpleDateFormat("yyyy-mm-dd").format(new Date());

		// bibs in index not voyager
		if (c.bibsInIndexNotVoyager.size() > 0) {
			System.out.println("bibids to be deleted from Solr");
			Integer[] ids = c.bibsInIndexNotVoyager.toArray(new Integer[ c.bibsInIndexNotVoyager.size() ]);
			Arrays.sort( ids );

			StringBuilder sb = new StringBuilder();
			for( Integer id: ids ) {
				System.out.println(id);
				sb.append(id);
				sb.append("\n");
			}

			String deleteReport = sb.toString();
			try {
				davService.saveFile(davUrl+"/updates/bib.deletes/bibListForDelete-"+ currentDate + ".txt",
						new ByteArrayInputStream(deleteReport.getBytes("UTF-8")));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		
		}

		
		// CurrentIndexMfhdList should now only contain mfhds to be deleted.
		if (c.mfhdsInIndexNotVoyager.size() > 0) {
			Iterator<Integer> bibids = c.mfhdsInIndexNotVoyager.values().iterator();
			Set<Integer> update_bibids = new TreeSet<Integer>();
			System.out.println("bibids to be updated in solr");
			while (bibids.hasNext())
				update_bibids.add(bibids.next());
			bibids = update_bibids.iterator();
			StringBuilder sb = new StringBuilder();
			while (bibids.hasNext()) {
				Integer bibid = bibids.next();
				System.out.println(bibid);
				sb.append(bibid);
				sb.append("\n");
			}

			String updateReport = sb.toString();
			try {
				davService.saveFile(davUrl+"/updates/bib.updates/bibListForUpdate-"+ currentDate + ".txt",
						new ByteArrayInputStream(updateReport.getBytes("UTF-8")));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		
	}
	
}
