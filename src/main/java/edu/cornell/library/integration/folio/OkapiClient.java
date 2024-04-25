package edu.cornell.library.integration.folio;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.rmi.NoSuchObjectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class OkapiClient {

	private final String url;
	private final String token;
	private final String tenant;

	public OkapiClient(String url, String tenant, String token, String user, String pass)
			throws IOException {
		this.url = url;
		this.tenant = tenant;
		if ( token != null)
			this.token = token;
		else {
			this.token = post("/authn/login",
					String.format("{\"username\":\"%s\",\"password\":\"%s\"}",user,pass));
		}
	}

	public String getToken() { return this.token; }

	public String post(final String endPoint, final String payload) throws IOException {
		return post(endPoint, payload, "application/json;charset=utf-8");
	}

	public String post(final String endPoint, final String payload, String contentType) throws IOException {

		System.out.println(endPoint+" POST");

		final URL fullPath = new URL(this.url + endPoint);
		final HttpURLConnection c = (HttpURLConnection) fullPath.openConnection();
		c.setRequestProperty("Content-Type", contentType);
		c.setRequestProperty("X-Okapi-Tenant", this.tenant);
		c.setRequestProperty("X-Okapi-Token", this.token);

		c.setRequestMethod("POST");
		c.setDoOutput(true);
		c.setDoInput(true);
		final OutputStreamWriter writer = new OutputStreamWriter(c.getOutputStream());
		writer.write(payload);
		writer.flush();
		//      int responseCode = httpConnection.getResponseCode();
		//      if (responseCode != 200)
		//          throw new IOException(httpConnection.getResponseMessage());

		String token = c.getHeaderField("x-okapi-token");

		final StringBuilder sb = new StringBuilder();
		try (BufferedReader br = new BufferedReader(new InputStreamReader(c.getInputStream(), "utf-8"))) {
			String line = null;
			while ((line = br.readLine()) != null) {
				sb.append(line + "\n");
			}
		}
		String response = sb.toString();
		if ( token != null )
			return token;
		return response;
	}

	public String put(String endPoint, Map<String, Object> object) throws IOException {
		return put(endPoint, (String) object.get("id"), mapper.writeValueAsString(object));
	}

	public String put(String endPoint, String uuid, String json) throws IOException {

		HttpURLConnection c = commonConnectionSetup(endPoint + "/" + uuid);
		c.setRequestMethod("PUT");
		c.setDoOutput(true);
		c.setDoInput(true);
		OutputStreamWriter writer = new OutputStreamWriter(c.getOutputStream());
		writer.write(json);
		writer.flush();
//      int responseCode = httpConnection.getResponseCode();
//      if (responseCode != 200)
//          throw new IOException(httpConnection.getResponseMessage());
		StringBuilder sb = new StringBuilder();
		try (BufferedReader br = new BufferedReader(new InputStreamReader(c.getInputStream(), "utf-8"))) {
			String line = null;
			while ((line = br.readLine()) != null) {
				sb.append(line + "\n");
			}
		}
		return sb.toString();
	}

	public String delete(String endPoint, String uuid) throws IOException {
		StringBuilder sb = delete(endPoint, uuid, new StringBuilder());
		return sb.toString();
	}

	public String deleteAll(String endPoint, boolean verbose) throws IOException {

		return deleteAll(endPoint, null, verbose);
	}

	public String deleteAll(String endPoint, String notDeletedQuery, boolean verbose) throws IOException {

		StringBuilder sb = new StringBuilder();

		Map<String, Map<String, Object>> existing = queryAsMap(endPoint, notDeletedQuery, null);

		while (!existing.isEmpty()) {
			String output = existing.keySet().parallelStream().map(uuid -> {
				try {
					return String.format("Deleting %s/%s %s\n", endPoint, uuid, delete(endPoint, uuid));
				} catch (Exception e) {
					return e.getMessage();
				}
			}).collect(Collectors.joining("\n"));
			if (verbose)
				System.out.println(output);
			existing = queryAsMap(endPoint, notDeletedQuery, null);

		}
		return sb.toString();
	}

	public String getRecord(String endPoint, String uuid) throws IOException {
		HttpURLConnection c = commonConnectionSetup(endPoint + "/" + uuid);
		int responseCode = c.getResponseCode();
		if (responseCode != 200)
			throw new NoSuchObjectException(c.getResponseMessage());

		try ( BufferedReader br = new BufferedReader(new InputStreamReader(c.getInputStream(), "utf-8")) ) {
			String line = null;
			StringBuilder response = new StringBuilder();
			while ((line = br.readLine()) != null) {
				response.append(line + "\n");
			}
			return response.toString();
		}
	}

	public List<Map<String, Object>> queryAsList(String endPoint, String query) throws IOException {
		return queryAsList(endPoint, query, null);
	}

	public List<Map<String, Object>> queryAsList(String endPoint, String query, Integer limit) throws IOException {
		return resultsToList(query(endPoint, query, limit));
	}

	public Map<String, Map<String, Object>> queryAsMap(String endPoint, String query, Integer limit) throws IOException {
		return resultsToMap(query(endPoint, query, limit));
	}

	public String query(String endPoint, String query, Integer limit) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append(endPoint);
		if (query != null) {
			sb.append("?query=");
			sb.append(URLEncoder.encode(query, "UTF-8"));
		}
		if (limit != null) {
			String limitField = endPoint.startsWith("/perms") ? "length" : "limit";
			sb.append((query == null) ? '?' : '&');
			sb.append(limitField);
			sb.append('=');
			sb.append(limit);
		}
		System.out.println(sb.toString());
		HttpURLConnection c = commonConnectionSetup(sb.toString());
		int responseCode = c.getResponseCode();
		if (responseCode != 200)
			throw new IOException(c.getResponseMessage());

		try ( BufferedReader br = new BufferedReader(new InputStreamReader(c.getInputStream(), "utf-8")) ) {
			String line = null;
			StringBuilder response = new StringBuilder();
			while ((line = br.readLine()) != null) {
				response.append(line + "\n");
			}
			return response.toString();
		}
	}

	public String query(String endPointQuery) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append(endPointQuery);
		System.out.println(sb.toString());
		HttpURLConnection c = commonConnectionSetup(sb.toString());
		int responseCode = c.getResponseCode();
		if (responseCode != 200)
			throw new IOException(c.getResponseMessage());

		try ( BufferedReader br = new BufferedReader(new InputStreamReader(c.getInputStream(), "utf-8")) ) {
			String line = null;
			StringBuilder response = new StringBuilder();
			while ((line = br.readLine()) != null) {
				response.append(line + "\n");
			}
			return response.toString();
		}
	}

	static final List<String> notRecordsKeys = Arrays.asList("totalRecords", "resultInfo", "pageSize", "page",
			"totalPages", "meta", "totalRecords", "total");

	@SuppressWarnings("unchecked")
	public static Map<String, Map<String, Object>> resultsToMap(String readValue)
			throws JsonParseException, JsonMappingException, IOException {
		Map<String, Map<String, Object>> dataMap = new HashMap<>();

		List<Map<String, Object>> records = null;
		if (readValue.startsWith("[")) {
			records = mapper.readValue(readValue, ArrayList.class);
		} else {
			Map<String, Object> rawData = mapper.readValue(readValue, Map.class);
			System.out.println(String.join(", ", rawData.keySet()));
			for (String mainKey : rawData.keySet())
				if (!notRecordsKeys.contains(mainKey)) {
					records = (ArrayList<Map<String, Object>>) rawData.get(mainKey);
					System.out.println(records);
				}
		}
		for (Map<String, Object> record : records) {
			System.out.println(record.get("name"));
			if (record.containsKey("name") && ((String) record.get("name")).contains("Test License"))
				continue;
			dataMap.put((String) record.get("id"), record);
		}
		return dataMap;
	}

	public static List<Map<String, Object>> resultsToList(String readValue)
			throws JsonParseException, JsonMappingException, IOException {
		if (readValue.startsWith("["))
			return mapper.readValue(readValue, ArrayList.class);
		Map<String, Object> rawData = mapper.readValue(readValue, Map.class);
		for (String mainKey : rawData.keySet()) {
			if (!mainKey.equals("totalRecords") && !mainKey.equals("resultInfo")) {
				@SuppressWarnings("unchecked")
				List<Map<String, Object>> records = (ArrayList<Map<String, Object>>) rawData.get(mainKey);
				return records;
			}
		}
		return null;
	}

	// END OF PUBLIC UTILITIES

	private HttpURLConnection commonConnectionSetup(String path) throws IOException {
		URL fullPath = new URL(this.url + path);
		HttpURLConnection c = (HttpURLConnection) fullPath.openConnection();
		c.setRequestProperty("Content-Type", "application/json;charset=utf-8");
		c.setRequestProperty("X-Okapi-Tenant", this.tenant);
		c.setRequestProperty("X-Okapi-Token", this.token);
		return c;

	}

	private StringBuilder delete(String endPoint, String uuid, StringBuilder sb) throws IOException {
		HttpURLConnection c = commonConnectionSetup(endPoint + "/" + uuid);
		c.setRequestMethod("DELETE");
//      int responseCode = httpConnection.getResponseCode();
//      if (responseCode != 200)
//          throw new IOException(httpConnection.getResponseMessage());
		try (BufferedReader br = new BufferedReader(new InputStreamReader(c.getInputStream(), "utf-8"))) {
			String line = null;
			while ((line = br.readLine()) != null) {
				sb.append(line + "\n");
			}
		}
		return sb;
	}

	private static ObjectMapper mapper = new ObjectMapper();
}
