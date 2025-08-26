package edu.cornell.library.integration.folio;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.rmi.NoSuchObjectException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
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

	private final String name;
	private final String url;
	private final String tenant;
	private final String username;
	private final String password;
	private String accessToken = null;
	private Instant accessExpires = null;
	private String refreshToken = null;
	private Instant refreshExpires = null;


	public OkapiClient(String name, String url, String tenant, String username, String password) throws IOException {
		this.name = name;
		this.url = url;
		this.tenant = tenant;
		this.username = username;
		this.password = password;
		HttpURLConnection c = this.post("/authn/login-with-expiry",String.format(
				"{\"username\":\"%s\",\"password\":\"%s\"}",this.username,this.password));
		if (201 != c.getResponseCode())
			throw new IOException(String.format("%s:%s %s",this.name,this.username,c.getResponseMessage()));
		parseLoginResponse(c);
	}

	public void confirmTokensCurrent() throws IOException {
		Instant inTwoMinutes = Instant.now().plus(2, ChronoUnit.MINUTES);
		if (this.accessToken != null && this.accessExpires != null) {

			// access token has at least 2 minutes left, so use it.
			if (this.accessExpires.isAfter(inTwoMinutes))
				return;

			// access token is old, but refresh token is still good, so refresh
			if (this.refreshToken != null && this.refreshExpires != null
					&& this.refreshExpires.isAfter(inTwoMinutes)) {
				refreshTokens();
				return;
			}
		}

		// login again
		HttpURLConnection c = this.post("/authn/login-with-expiry",String.format(
				"{\"username\":\"%s\",\"password\":\"%s\"}",this.username,this.password));
		if (201 != c.getResponseCode())
			throw new IOException(String.format("%s:%s %s",this.name,this.username,c.getResponseMessage()));
		parseLoginResponse(c);
	}

	public void refreshTokens() throws IOException {
		Map<String,String> headers = new HashMap<>();
		headers.put("Cookie", String.format("folioRefreshToken=%s; folioAccessToken=%s", this.refreshToken,this.accessToken));

		HttpURLConnection c = post("/authn/refresh","", headers);
		if (201 != c.getResponseCode())
			throw new IOException(String.format("%s:%s %s",this.name,this.username,c.getResponseMessage()));
		parseLoginResponse(c);
	}

	private void parseLoginResponse(HttpURLConnection c) throws IOException {
		for (String cookie : c.getHeaderFields().get("Set-Cookie")) {
			if (cookie.startsWith("folioAccessToken"))
				this.accessToken = cookie.substring(17,cookie.indexOf(';'));
			if (cookie.startsWith("folioRefreshToken"))
				this.refreshToken = cookie.substring(18,cookie.indexOf(';'));
		}
		Map<String,Object> response = mapper.readValue(convertStreamToString(c.getInputStream()), Map.class);
		this.accessExpires = isoDT.parse((String)response.get("accessTokenExpiration"), Instant::from);
		this.refreshExpires = isoDT.parse((String)response.get("refreshTokenExpiration"), Instant::from);
	}
	private static final DateTimeFormatter isoDT = DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of("Z"));

	public HttpURLConnection post(final String endPoint, final String json) throws IOException {
		return post(endPoint,json,null);
	}

	public HttpURLConnection post(final String endPoint, final String json, Map<String,String> headers) throws IOException {

		System.out.println(endPoint+" (post)");

		final URL fullPath = new URL(this.url + endPoint);
		final HttpURLConnection c = (HttpURLConnection) fullPath.openConnection();
		c.setRequestProperty("Content-Type", "application/json;charset=utf-8");
		c.setRequestProperty("X-Okapi-Tenant", this.tenant);
		if (this.accessToken != null)
			c.setRequestProperty("X-Okapi-Token", this.accessToken);
		if (headers != null)
			for (String headerName : headers.keySet())
				c.setRequestProperty(headerName, headers.get(headerName));

		c.setRequestMethod("POST");
		c.setDoOutput(true);
		c.setDoInput(true);
		final OutputStreamWriter writer = new OutputStreamWriter(c.getOutputStream());
		writer.write(json);
		writer.flush();

		return c;
	}

	public String put(String endPoint, Map<String, Object> object) throws IOException {
		return put(endPoint, (String) object.get("id"), mapper.writeValueAsString(object));
	}

	public String put(String endPoint, String uuid, String json) throws IOException {

		confirmTokensCurrent();
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
		confirmTokensCurrent();
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
		confirmTokensCurrent();
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
		confirmTokensCurrent();
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
		System.out.println(fullPath.toString());
		HttpURLConnection c = (HttpURLConnection) fullPath.openConnection();
		c.setRequestProperty("Content-Type", "application/json;charset=utf-8");
		c.setRequestProperty("X-Okapi-Tenant", this.tenant);
		c.setRequestProperty("X-Okapi-Token", this.accessToken);
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

	private static String convertStreamToString(final java.io.InputStream is) {
		try (java.util.Scanner s = new java.util.Scanner(is)) {
			s.useDelimiter("\\A");
			return s.hasNext() ? s.next() : "";
		}
	}


	private static ObjectMapper mapper = new ObjectMapper();


	public void printLoginStatus(OkapiClient folio) {
		Instant now = Instant.now();
		System.out.format("%s:%s; ACCESS: %s; REFRESH: %s\n",
				this.name,this.username,
				humanReadableTimespan(now.until(accessExpires,ChronoUnit.SECONDS)),
				humanReadableTimespan(now.until(refreshExpires,ChronoUnit.SECONDS)));
	}

	private String humanReadableTimespan(long seconds) {
		int minuteLen = 60, hourLen = 3600, dayLen = 86_400;
		if (seconds > dayLen) {
			long days = seconds / (dayLen);
			long hours = (seconds-(dayLen*days))/(hourLen);
			return String.format("%d days, %d hours", days, hours);
		}
		if (seconds > hourLen) {
			long hours = seconds / (hourLen);
			long minutes = (seconds-(hourLen*hours))/minuteLen;
			return String.format("%d hours, %d minutes", hours, minutes);
		}
		if (seconds > 60) {
			long minutes = seconds / minuteLen;
			return String.format("%d minutes, %d seconds", minutes, seconds - (minuteLen*minutes));
		}
		return seconds + " seconds";
	}
}
