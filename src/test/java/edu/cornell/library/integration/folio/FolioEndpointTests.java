package edu.cornell.library.integration.folio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

import com.fasterxml.jackson.databind.ObjectMapper;

import static edu.cornell.library.integration.folio.DownloadMARC.jsonToMarcRec;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.utilities.Config;

public class FolioEndpointTests {

	public static void main(String[] args) throws IOException {

		Map<String, String> env = System.getenv();
		String folioConfig = env.get("target_folio");
		if ( folioConfig == null )
			throw new IllegalArgumentException("target_folio must be set in environment to name of target Folio instance.");
		Config config = Config.loadConfig(new HashSet<>());
		FolioClient folio = config.getFolio(folioConfig);
		Random random = new Random();

		folio.printLoginStatus();
		System.out.println("We want to hit every endpoint used by the indexing process with a query "
				+ "that exemplifies the way those API's will be used in production. Because this is "
				+ "integration testing, it depends on data in live Folio instances, so the tests may need "
				+ "to be updated if the expected data are missing. Reference Data endpoints are testing for "
				+ "a generally appropriate number of entries.\n");

		System.out.println("\n**TESTS FOR Monitor Folio job**");
		System.out.println("Many of these queries use the timeframe of the last 10 hours to look for endpoint changes.");
		System.out.println("Those queries will retrieve up to 10 changed records, which is far fewer than the"
				+ " production queries, which should otherwise be the same.\n");
		Timestamp tenHoursAgo = new Timestamp(Calendar.getInstance().getTime().getTime()-(10*60*60*1000));
		holdingsStorageByDateTest(folio, tenHoursAgo);
		instanceStorageByDateTest(folio, tenHoursAgo);
		itemStorageByDateTest(folio, tenHoursAgo);
		loanStorageByDateTest(folio, tenHoursAgo);
		ordersStoragePOLinesByDateTest(folio, tenHoursAgo);
		ordersStoragePOsByDateTest(folio, tenHoursAgo);
		requestsStorageByDateTest(folio, tenHoursAgo);
		sourceStorageByInstanceIdTest(folio);

		System.out.println("\n\n**TESTS FOR Metadata Generation Job**");
		System.out.println("This job only accesses reference data sets.");
		callNumberTypesTest(folio, random);
		contributorNameTypesTest(folio, random);
		contributorTypesTest(folio, random);
		instanceNoteTypesTest(folio, random);
		instanceStatusesTest(folio, random);
		instanceTypesTest(folio, random);
		locationsTest(folio, random);
		statisticalCodesTest(folio, random);

		System.out.println("\n\n**TESTS FOR Availability data and Solr update Job**");
		System.out.println("This job only accesses reference data sets.");
		callNumberTypesTest(folio, random); // also in previous test set
		holdingsNoteTypesTest(folio, random);
		holdingsNoteTypesTest(folio, random);
		itemNoteTypesTest(folio, random);
		loanTypesTest(folio, random);
		locationsTest(folio, random); // also in previous test set
		libraryLocationsTest(folio, random);
		materialTypesTest(folio, random);
		servicePointsTest(folio, random);
		statisticalCodesTest(folio, random); // also in previous test set

	}
	private static void holdingsStorageByDateTest(FolioClient folio, Timestamp since) throws IOException {
		System.out.println("Recently changed holdings test...");
		String output = folio.query("/holdings-storage/holdings",
				"metadata.updatedDate>"+since.toInstant().toString()+
				" sortBy metadata.updatedDate",10);
		Map<String, Object> rawData = mapper.readValue(output, Map.class);
		assertTrue(rawData.containsKey("holdingsRecords"));
		assertTrue(rawData.containsKey("totalRecords"));
		assertTrue(rawData.containsKey("resultInfo"));
		assertInstanceOf(Integer.class, rawData.get("totalRecords"));
		assertInstanceOf(ArrayList.class, rawData.get("holdingsRecords"));
		int records = (int) rawData.get("totalRecords");
		assertEquals( Math.min(records, 10), ((ArrayList)rawData.get("holdingsRecords")).size());
		System.out.println(records+" changed holdings identified. Response correctly structured.\n");
	}

	private static void instanceStorageByDateTest(FolioClient folio, Timestamp since) throws IOException {
		System.out.println("Recently changed instances test...");
		String output = folio.query("/instance-storage/instances",
				"metadata.updatedDate>"+since.toInstant().toString()+
				" sortBy metadata.updatedDate",10);
		Map<String, Object> rawData = mapper.readValue(output, Map.class);
		assertTrue(rawData.containsKey("instances"));
		assertTrue(rawData.containsKey("totalRecords"));
		assertTrue(rawData.containsKey("resultInfo"));
		assertInstanceOf(Integer.class, rawData.get("totalRecords"));
		assertInstanceOf(ArrayList.class, rawData.get("instances"));
		int records = (int) rawData.get("totalRecords");
		assertEquals( Math.min(records, 10), ((ArrayList)rawData.get("instances")).size());
		System.out.println(records+" changed instance identified. Response correctly structured.\n");
	}

	private static void itemStorageByDateTest(FolioClient folio, Timestamp since) throws IOException {
		System.out.println("Recently changed items test...");
		String output = folio.query("/item-storage/items",
				"metadata.updatedDate>"+since.toInstant().toString()+
				" sortBy metadata.updatedDate",10);
		Map<String, Object> rawData = mapper.readValue(output, Map.class);
		assertTrue(rawData.containsKey("items"));
		assertTrue(rawData.containsKey("totalRecords"));
		assertTrue(rawData.containsKey("resultInfo"));
		assertInstanceOf(Integer.class, rawData.get("totalRecords"));
		assertInstanceOf(ArrayList.class, rawData.get("items"));
		int records = (int) rawData.get("totalRecords");
		assertEquals( Math.min(records, 10), ((ArrayList)rawData.get("items")).size());
		System.out.println(records+" changed items identified. Response correctly structured.\n");
	}

	private static void loanStorageByDateTest(FolioClient folio, Timestamp since) throws IOException {
		System.out.println("Recently changed loans test...");
		String output = folio.query("/loan-storage/loans",
				"metadata.updatedDate>"+since.toInstant().toString()+
				" sortBy metadata.updatedDate",10);
		Map<String, Object> rawData = mapper.readValue(output, Map.class);
		assertTrue(rawData.containsKey("loans"));
		assertTrue(rawData.containsKey("totalRecords"));
		assertInstanceOf(Integer.class, rawData.get("totalRecords"));
		assertInstanceOf(ArrayList.class, rawData.get("loans"));
		int records = (int) rawData.get("totalRecords");
		assertEquals( Math.min(records, 10), ((ArrayList)rawData.get("loans")).size());
		System.out.println(records+" changed loans identified. Response correctly structured.\n");
	}

	private static void ordersStoragePOLinesByDateTest(FolioClient folio, Timestamp since) throws IOException {
		System.out.println("Recently changed order PO lines test...");
		String output = folio.query("/orders-storage/po-lines",
				"metadata.updatedDate>"+since.toInstant().toString()+
				" sortBy metadata.updatedDate",10);
		Map<String, Object> rawData = mapper.readValue(output, Map.class);
		assertTrue(rawData.containsKey("poLines"));
		assertTrue(rawData.containsKey("totalRecords"));
		assertInstanceOf(Integer.class, rawData.get("totalRecords"));
		assertInstanceOf(ArrayList.class, rawData.get("poLines"));
		int records = (int) rawData.get("totalRecords");
		assertEquals( Math.min(records, 10), ((ArrayList)rawData.get("poLines")).size());
		System.out.println(records+" changed order PO lines identified. Response correctly structured.\n");
	}

	private static void ordersStoragePOsByDateTest(FolioClient folio, Timestamp since) throws IOException {
		System.out.println("Recently changed purchase orders test...");
		String output = folio.query("/orders-storage/purchase-orders",
				"metadata.updatedDate>"+since.toInstant().toString()+
				" sortBy metadata.updatedDate",10);
		Map<String, Object> rawData = mapper.readValue(output, Map.class);
		assertTrue(rawData.containsKey("purchaseOrders"));
		assertTrue(rawData.containsKey("totalRecords"));
		assertInstanceOf(Integer.class, rawData.get("totalRecords"));
		assertInstanceOf(ArrayList.class, rawData.get("purchaseOrders"));
		int records = (int) rawData.get("totalRecords");
		assertEquals( Math.min(records, 10), ((ArrayList)rawData.get("purchaseOrders")).size());
		System.out.println(records+" changed purchase orders identified. Response correctly structured.\n");
	}

	private static void requestsStorageByDateTest(FolioClient folio, Timestamp since) throws IOException {
		System.out.println("Recently updated requests test...");
		String output = folio.query("/request-storage/requests",
				"metadata.updatedDate>"+since.toInstant().toString()+
				" sortBy metadata.updatedDate",10);
		Map<String, Object> rawData = mapper.readValue(output, Map.class);
		assertTrue(rawData.containsKey("requests"));
		assertTrue(rawData.containsKey("totalRecords"));
		assertInstanceOf(Integer.class, rawData.get("totalRecords"));
		assertInstanceOf(ArrayList.class, rawData.get("requests"));
		int records = (int) rawData.get("totalRecords");
		assertEquals( Math.min(records, 10), ((ArrayList)rawData.get("requests")).size());
		System.out.println(records+" changed requests identified. Response correctly structured.\n");
	}

	private static void sourceStorageByInstanceIdTest(FolioClient folio) throws IOException {
		String instanceId = "c8e69b87-6771-4a79-895c-119a92916fa7";
		System.out.println("Specific source record retrieval by instance id test...");
		String output = folio.query("/source-storage/records/"+instanceId+"/formatted?idType=INSTANCE");
		MarcRecord record = jsonToMarcRec(output);
		assertEquals("2", record.id);
		for (DataField f : record.dataFields)
			if (f.tag.equals("245")) {
				assertEquals("245 10 ‡a Planning elementary buildings for school and community use /"
						+ " ‡c Arthur W. Clevenger.",f.toString());
				System.out.format("bib: %s, title: %s\n",record.id, f);
			}

	}

	private static void callNumberTypesTest(FolioClient folio, Random random) throws IOException {
		ReferenceData callNumberTypes = new ReferenceData(folio,"/call-number-types","name");
		Object[] values = callNumberTypes.dataByName.entrySet().toArray();
		System.out.format("names: %d; random name: %s\n", values.length,values[random.nextInt(values.length)]);
		assertTrue( values.length > 10);
	}

	private static void contributorNameTypesTest(FolioClient folio, Random random) throws IOException {
		ReferenceData contributorNameTypes = new ReferenceData( folio, "/contributor-name-types","ordering");
		Object[] values = contributorNameTypes.dataByName.entrySet().toArray();
		System.out.format("orderings: %d; random ordering: %s\n", values.length,values[random.nextInt(values.length)]);
		assertEquals( 3, values.length );
	}

	private static void contributorTypesTest(FolioClient folio, Random random) throws IOException {
		ReferenceData contributorTypes = new ReferenceData( folio, "/contributor-types","name");
		Object[] values = contributorTypes.dataByName.entrySet().toArray();
		System.out.format("names: %d; random name: %s\n", values.length,values[random.nextInt(values.length)]);
		assertTrue( values.length > 250);
	}

	private static void instanceNoteTypesTest(FolioClient folio, Random random) throws IOException {
		ReferenceData instanceNoteTypes = new ReferenceData( folio, "/instance-note-types","name");
		Object[] values = instanceNoteTypes.dataByName.entrySet().toArray();
		System.out.format("names: %d; random name: %s\n", values.length,values[random.nextInt(values.length)]);
		assertTrue( values.length > 50);
	}

	private static void instanceStatusesTest(FolioClient folio, Random random) throws IOException {
		ReferenceData instanceStatuses = new ReferenceData( folio, "/instance-statuses","code");
		Object[] values = instanceStatuses.dataByName.entrySet().toArray();
		System.out.format("codes: %d; random code: %s\n", values.length,values[random.nextInt(values.length)]);
		assertTrue( values.length >= 5);
	}

	private static void instanceTypesTest(FolioClient folio, Random random) throws IOException {
		ReferenceData instanceTypes = new ReferenceData( folio, "/instance-types","name");
		Object[] values = instanceTypes.dataByName.entrySet().toArray();
		System.out.format("names: %d; random name: %s\n", values.length,values[random.nextInt(values.length)]);
		assertTrue( values.length >= 25);
	}

	private static void locationsTest(FolioClient folio, Random random) throws IOException {
		ReferenceData locations = new ReferenceData( folio,"/locations","code");
		Object[] values = locations.dataByName.entrySet().toArray();
		System.out.format("codes: %d; random code: %s\n", values.length,values[random.nextInt(values.length)]);
		assertTrue( values.length >= 200);
	}

	private static void statisticalCodesTest(FolioClient folio, Random random) throws IOException {
		ReferenceData statCodes = new ReferenceData(folio,"/statistical-codes","code");
		Object[] values = statCodes.dataByName.entrySet().toArray();
		System.out.format("codes: %d; random code: %s\n", values.length,values[random.nextInt(values.length)]);
		assertTrue( values.length > 70);
	}

	private static void holdingsNoteTypesTest(FolioClient folio, Random random) throws IOException {
		ReferenceData holdingsNoteTypes = new ReferenceData(folio, "/holdings-note-types", "name");
		Object[] values = holdingsNoteTypes.dataByName.entrySet().toArray();
		System.out.format("names: %d; random name: %s\n", values.length,values[random.nextInt(values.length)]);
		assertTrue( values.length > 10);
	}

	private static void itemNoteTypesTest(FolioClient folio, Random random) throws IOException {
		ReferenceData itemNoteTypes = new ReferenceData(folio, "/item-note-types", "name");
		Object[] values = itemNoteTypes.dataByName.entrySet().toArray();
		System.out.format("names: %d; random name: %s\n", values.length,values[random.nextInt(values.length)]);
		assertTrue( values.length > 10);
	}

	private static void loanTypesTest(FolioClient folio, Random random) throws IOException {
		ReferenceData loanTypes = new ReferenceData(folio, "/loan-types", "name");
		Object[] values = loanTypes.dataByName.entrySet().toArray();
		System.out.format("names: %d; random name: %s\n", values.length,values[random.nextInt(values.length)]);
		assertTrue( values.length > 10);
	}

	private static void libraryLocationsTest(FolioClient folio, Random random) throws IOException {
		ReferenceData libraries = new ReferenceData(folio, "/location-units/libraries", "name");
		Object[] values = libraries.dataByName.entrySet().toArray();
		System.out.format("names: %d; random name: %s\n", values.length,values[random.nextInt(values.length)]);
		assertTrue( values.length > 20);
	}

	private static void materialTypesTest(FolioClient folio, Random random) throws IOException {
		ReferenceData materialTypes = new ReferenceData(folio, "/material-types", "name");
		Object[] values = materialTypes.dataByName.entrySet().toArray();
		System.out.format("names: %d; random name: %s\n", values.length,values[random.nextInt(values.length)]);
		assertTrue( values.length > 20);
	}

	private static void servicePointsTest(FolioClient folio, Random random) throws IOException {
		ReferenceData materialTypes = new ReferenceData(folio, "/service-points", "name");
		Object[] values = materialTypes.dataByName.entrySet().toArray();
		System.out.format("names: %d; random name: %s\n", values.length,values[random.nextInt(values.length)]);
		assertTrue( values.length > 35);
	}

	private static ObjectMapper mapper = new ObjectMapper();

}
