package edu.cornell.library.integration.folio;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ReferenceData {


	// PUBLIC METHODS

	/*
	 * Retrieve the complete data set (up to 4000) items from OKAPI, and build a
	 * reference map to identify UUIDs based on key values. The UUIDs are assumed to
	 * be the "id" field in the data set, and the provided key field is the field
	 * that will be used to find the UUID values. This currently only works with top
	 * level key fields, and those where the key field values are strings. If other
	 * use cases arise, this can be expanded.
	 */
	public ReferenceData(OkapiClient okapi, String endPoint, String nameField) throws IOException {
		this(okapi.query(endPoint , null, 4000), nameField);
	}
	
	public ReferenceData(String json, String nameField) throws IOException {
	    JsonNode rawData = mapper.readTree(json);
	    Map<String, String> processedByName = new HashMap<>();
        Map<String, String> processedByUuid = new HashMap<>();

        // check for any required fields?
        rawData.fieldNames().forEachRemaining(mainKey -> {
            JsonNode mainValue = rawData.get(mainKey);
            if (!mainKey.equals("totalRecords") && mainValue.isArray()) {
                mainValue.iterator().forEachRemaining(item -> {
                    String name = item.get(nameField).asText();
                    String id = item.get("id").asText();
                    processedByName.put( (name).toLowerCase(), id);
                    processedByUuid.put(id, name);
                });
            }
        });
        
        this.dataByName = processedByName;
        this.dataByUuid = processedByUuid;
        this.entriesByUuid = new HashMap<>();
        for ( Entry<String,String> e : this.dataByUuid.entrySet() ) {
            Map<String,String> entry = new HashMap<>();
            entry.put("id", e.getKey());
            entry.put(nameField, e.getValue());
            this.entriesByUuid.put(e.getKey(), entry);
        }
	}

	/* 
	 * Instantiate empty ReferenceData for testing
	 */
	public ReferenceData(String keyField) {
		this.defaultKey = keyField;
		this.entriesByUuid = new HashMap<>();
		this.dataByUuid = new HashMap<>();
		this.dataByName = new HashMap<>();
	}
	public void addTestValue( String uuid, String value ) {
		this.dataByName.put(value, uuid);
		this.dataByUuid.put(uuid, value);
		Map<String,String> entry = new HashMap<>();
		entry.put("id",uuid);
		entry.put(this.defaultKey, value);
		this.entriesByUuid.put(uuid, entry);
	}
	
	/*
	 * Get the UUID for the given key value. If the key value isn't populated,
	 * return default or null.
	 */
	public String getUuid(String keyValue) {
		if (keyValue == null) return null;
		String value = keyValue.toLowerCase();
		if (this.defaultKey == null || this.dataByName.containsKey(value))
			return this.dataByName.get(value);
		return this.dataByName.get(this.defaultKey);
	}

	/*
	 * Get the name value for uuid. Return null if unpopulated.
	 */
	public String getName(String uuid) {
		if (this.defaultKey == null || this.dataByUuid.containsKey(uuid))
			return this.dataByUuid.get(uuid);
		return null;
	}
	/*
	 * Get the UUID for the given key value. If key value isn't populated, return
	 * null.
	 */
	public String getStrictUuid(String keyValue) {
		return this.dataByName.get(keyValue.toLowerCase());
	}

	public Map<String,String> getEntryHashByUuid( String uuid ) {
		if ( this.entriesByUuid.containsKey(uuid))
			return this.entriesByUuid.get(uuid);
		return null;
	}


	/* Set default key value to use when invalid key is requested. Throws
	 * IllegalArgumentException if invalid.
	 */
	public void setDefault(String defaultKey) throws IllegalArgumentException {
		String value = defaultKey.toLowerCase();
		if (value != null && !this.dataByName.containsKey(value))
			throw new IllegalArgumentException("Default key \"" + value + "\" is not a valid key.");
		this.defaultKey = value;
	}

	public void writeMapToStdout() {
		for (Entry<String, String> e : this.dataByName.entrySet())
			System.out.printf("%s => %s\n", e.getKey(), e.getValue());
	}

	// PRIVATE VALUES AND METHODS

	private final Map<String, String> dataByName;
	private final Map<String, String> dataByUuid;
	private final Map<String, Map<String,String>> entriesByUuid;
	private static ObjectMapper mapper = new ObjectMapper();
	private String defaultKey = null;

}
