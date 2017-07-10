package edu.cornell.library.integration.indexer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.solrFieldGen.*;
import edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.SolrFields;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class GenerateSolrFields {

	private static List<Class<? extends SolrFieldGenerator>> classes =
			Arrays.<Class<? extends SolrFieldGenerator>>asList(
					AuthorTitle.class,
					Subject.class,
					SimpleProc.class);

	// keyed by generator class simple names
	private static Map<String,SolrFieldGenerator> generators = new HashMap<>();

	// keyed by MARC field, generator class simple names as values
	private static Map<String,List<String>> fieldsSupported = new HashMap<>();

	static {
		for (Class<? extends SolrFieldGenerator> generatorClass : classes) {
			String simpleName = generatorClass.getSimpleName();
			try {
				SolrFieldGenerator gen = generatorClass.newInstance();
				generators.put(simpleName,gen);
				List<String> classFieldsSupported = gen.getHandledFields();
				for (String field : classFieldsSupported) {
					if ( ! fieldsSupported.containsKey(field) )
						fieldsSupported.put(field, new ArrayList<String>());
					fieldsSupported.get(field).add(simpleName);
				}
			} catch (InstantiationException | IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(fieldsSupported));
	}

	public static void generateSolr( MarcRecord rec, SolrBuildConfig config ) {
		ObjectMapper mapper = new ObjectMapper();
		Map<String,MarcRecord> recordChunks = new HashMap<>();
		for (ControlField f : rec.controlFields)
			if (fieldsSupported.containsKey(f.tag)) {
				for( String supportingClass : fieldsSupported.get(f.tag)) {
					if ( ! recordChunks.containsKey(supportingClass) ) {
						recordChunks.put(supportingClass, new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC ));
						recordChunks.get(supportingClass).id = rec.id;
					}
					recordChunks.get(supportingClass).controlFields.add(f);
				}
			}
		for (DataField f : rec.dataFields)
			if (fieldsSupported.containsKey(f.tag)) {
				for( String supportingClass : fieldsSupported.get(f.tag)) {
					if ( ! recordChunks.containsKey(supportingClass) ) {
						recordChunks.put(supportingClass, new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC ));
						recordChunks.get(supportingClass).id = rec.id;
					}
					recordChunks.get(supportingClass).dataFields.add(f);
				}
			}
		try {
			for (Entry<String,MarcRecord> e : recordChunks.entrySet()) {
				SolrFields sfs = generators.get(e.getKey()).generateSolrFields(e.getValue(), config);
				System.out.println(e.getKey());
				System.out.println(e.getValue().toString());
				System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(sfs.getFields()));
			}
		} catch (ClassNotFoundException | SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
