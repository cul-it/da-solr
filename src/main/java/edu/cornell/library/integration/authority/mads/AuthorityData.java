package edu.cornell.library.integration.authority.mads;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.apicatalog.jsonld.JsonLdError;

import edu.cornell.library.integration.authority.AuthoritySource;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;

public class AuthorityData {
	public String id = null;
	public String lccn = null;
	public AuthoritySource vocab = null;
	public RecordStatus recordStatus = null;
	public String authorativeLabel = null;
	public HeadingType headingType = null;
	public boolean isSubdivision = false;
	public boolean undifferentiated = false;
	public String moddate = null;
	public int numUpdates = 0;
	public String source = null;
	public String addedDate = null;
	public JsonObject mainEntry = null;
	public JsonArray graph = null;

	public void print() {
		System.out.println("ID: " + id);
		System.out.println("LCCN: " + lccn);
		System.out.println("Vocab: " + vocab);
		System.out.println("Record status: " + recordStatus.getDisplay());
		System.out.println("authorativeLabel: " + authorativeLabel);
		System.out.println("Heading type: " + headingType);
		System.out.println("Subdivision: " + isSubdivision);
		System.out.println("Undifferentiated: " + undifferentiated);
		System.out.println("Mod date: " + moddate);
		System.out.println("Num updates: " + numUpdates);
		System.out.println("Source: " + source);
		System.out.println("Added date: " + addedDate);
	}

	public String catalogId() throws URISyntaxException {
		URI uri = new URI(id);
		String path = uri.getPath();
		return path.substring(path.lastIndexOf('/') + 1);
	}

	public static AuthorityData fromDbRecord(ResultSet rs) throws SQLException, JsonLdError {
		AuthorityData apd = new AuthorityData();
		apd.id = rs.getString("id");
		apd.lccn = rs.getString("lccn");
		apd.vocab = AuthoritySource.byOrdinal(rs.getInt("vocabulary"));
		apd.recordStatus = RecordStatus.byOrdinal(rs.getInt("recordStatus"));
		apd.authorativeLabel = rs.getString("heading");
		apd.headingType = HeadingType.byOrdinal(rs.getInt("headingType"));
		apd.isSubdivision = rs.getBoolean("isSubdivision");
		apd.undifferentiated = rs.getBoolean("undifferentiated");
		apd.moddate = rs.getString("moddate");
		apd.numUpdates = rs.getInt("numUpdates");
		apd.source = rs.getString("source");
		apd.addedDate = rs.getString("addedDate");
		apd.init();
		return apd;
	}

	protected void init() throws JsonLdError {
		JsonObject doc = JsonldUtils.parseJsonLd(source);
		graph = doc.getJsonArray("@graph");
		mainEntry = JsonldUtils.getJsonObjectForId(graph, id);
	}
}
