package edu.cornell.library.integration.folio;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Scanner;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReferenceDataTest {
	private String resourceDataJson = null;
	
	@BeforeEach
	public void setup() throws IOException {
		resourceDataJson = loadResourceFile("edu/cornell/library/integration/folio/instanceResourceTypes.json");
	}
	
	// create test utility class to share this function?
	public String loadResourceFile(String filename) throws IOException {
    	try ( InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
				Scanner s = new Scanner(is,"UTF-8")) {
			return s.useDelimiter("\\A").next();
		}
    }

    @Test
    void testName() throws IOException {
        ReferenceData rd = new ReferenceData(resourceDataJson, "name");
        
        String expectedName = "cartographic dataset";
        String gotName = rd.getName("3363cdb1-e644-446c-82a4-dc3a1d4395b9");
        assertEquals(expectedName, gotName);
        
        String expectedUuid = "df5dddff-9c30-4507-8b82-119ff972d4d7";
        String gotUuid = rd.getUuid("computer dataset");
        assertEquals(expectedUuid, gotUuid);
    }
    
    @Test
    void testCode() throws IOException {
        ReferenceData rd = new ReferenceData(resourceDataJson, "code");
        
        String expectedName = "zzz";
        String gotName = rd.getName("30fffe0e-e985-4144-b2e2-1e8179bdb41f");
        assertEquals(expectedName, gotName);
        
        String expectedUuid = "526aa04d-9289-4511-8866-349299592c18";
        String gotUuid = rd.getStrictUuid("cri");
        assertEquals(expectedUuid, gotUuid);
    }
    
    @Test
    void testGetEntryHashByUuid() throws IOException {
    	ReferenceData rd = new ReferenceData(resourceDataJson, "name");
    	String uuid = "bf8fa535-4097-4efe-8fed-75d22284997e";
    	Map<String, String> got = rd.getEntryHashByUuid(uuid);
    	
    	String expectedName = "Text (Check 336$b)";
    	assertEquals(expectedName, got.get("name"));
    	
    	String expectedId = "bf8fa535-4097-4efe-8fed-75d22284997e";
    	assertEquals(expectedId, got.get("id"));
    }
}
