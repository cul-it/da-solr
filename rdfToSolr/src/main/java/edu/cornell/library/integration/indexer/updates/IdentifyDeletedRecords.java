package edu.cornell.library.integration.indexer.updates;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import edu.cornell.library.integration.indexer.utilies.*;

public class IdentifyDeletedRecords {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		//should be args
		String coreUrl = "http://fbw4-dev.library.cornell.edu:8080/solr/test";
		Path currentVoyagerBibList = Paths.get("/users/fbw4/voyager-harvest/data/fulldump/bibs/unsuppressedBibId.txt");
		Path currentVoyagerMfhdList = Paths.get("/users/fbw4/voyager-harvest/data/fulldump/mfhds/unsuppressedMfhdId.txt");

		IndexRecordListComparison c = new IndexRecordListComparison();
		c.compare(coreUrl, currentVoyagerBibList, currentVoyagerMfhdList);

		// bibs in index not voyager
		if (c.bibsInIndexNotVoyager.size() > 0) {
			System.out.println("bibids to be deleted from Solr");
			Integer[] ids = c.bibsInIndexNotVoyager.toArray(new Integer[ c.bibsInIndexNotVoyager.size() ]);
			Arrays.sort( ids );
			for( Integer id: ids )
				System.out.println(id);
		}

		
		// CurrentIndexMfhdList should now only contain mfhds to be deleted.
		if (c.mfhdsInIndexNotVoyager.size() > 0) {
			Iterator<Integer> bibids = c.mfhdsInIndexNotVoyager.values().iterator();
			Set<Integer> update_bibids = new TreeSet<Integer>();
			System.out.println("bibids to be updated in solr");
			while (bibids.hasNext())
				update_bibids.add(bibids.next());
			bibids = update_bibids.iterator();
			while (bibids.hasNext())
				System.out.println(bibids.next());
		}

		
	}

}
