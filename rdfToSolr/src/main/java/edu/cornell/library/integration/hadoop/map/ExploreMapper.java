package edu.cornell.library.integration.hadoop.map;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * Mapper for exploring system properties. Doesn't do anything else interesting.
 * 
 * @author bdc34
 */
public class ExploreMapper  <K> extends Mapper<K, Text, Text, Text>{ 			
	org.apache.commons.logging.Log log = LogFactory.getLog(ExploreMapper.class);

	@Override
	public void map(K key, Text value, Context context) throws IOException, InterruptedException {
		log.error("This is the explore mapper, DO NOT USE IN REAL JOBS" );
				
		log.error("System Properties:"); 
		
		File f = new File("ExplorerMapperFile.txt");
		f.createNewFile();
		
		Properties p = System.getProperties();
		for(Object k:  p.keySet()){			
			log.error(k + ": " + p.getProperty((String) k));
		}		
		
		Configuration conf = new Configuration();
	    Iterator it = conf.iterator();
	    while (it.hasNext()) {
	      log.error( it.next() );
	    }	    
	    
	    TaskAttemptID attempt = context.getTaskAttemptID();		
		log.error("AttemptId: " + attempt.getId());
		log.error("JobId: " + attempt.getJobID());
		log.error("TaskId: " + attempt.getTaskID());
		// these lines output:
		// 12/12/06 12:54:43 ERROR map.ExploreMapper: AttemptId: 0
		// 12/12/06 12:54:43 ERROR map.ExploreMapper: JobId: job_local_0001
		// 12/12/06 12:54:43 ERROR map.ExploreMapper: TaskId: task_local_0001_m_000000

	}
	
}
