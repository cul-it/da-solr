package edu.cornell.library.integration.folio;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import edu.cornell.library.integration.marc.MarcRecord;

public class DataExport {

	public static List<MarcRecord> retrieveMarcByUuid( OkapiClient okapi, List<String> instanceUuids )
			throws IOException, InterruptedException {

		// IDENTIFY PROFILE
		List<Map<String, Object>> profiles = okapi.queryAsList("/data-export/job-profiles",
				"name==\"Default instances export job profile\"");
		if ( profiles.size() != 1) {
			System.out.println("Expected exactly one matching profile. Found "+profiles.size());
			for (Map<String,Object> profile : profiles) System.out.println(profile);
			return null;
		}
		String profileId = (String) profiles.get(0).get("id");

		// CREATE FILE DEFINITION
		Map<String,String> fdPayload = new HashMap<>();
		fdPayload.put("fileName", "test.csv");
		fdPayload.put("uploadFormat", "csv");
		String fileDefinitionId = (String)mapper.readValue(
				okapi.postToString("/data-export/file-definitions",
						mapper.writeValueAsString(fdPayload),null,null), Map.class).get("id");

		// UPLOAD FILE
		String csv = String.join("\n", instanceUuids);
		String uploadUrl = String.format("/data-export/file-definitions/%s/upload", fileDefinitionId);
		String output = okapi.postToString(uploadUrl, csv, null, "application/octet-stream");
		String jobExecutionId = (String)mapper.readValue(output, Map.class).get("jobExecutionId");

		// TRIGGER JOB
		Map<String,String> tPayload = new HashMap<>();
		tPayload.put("fileDefinitionId", fileDefinitionId);
		tPayload.put("jobProfileId", profileId);
		tPayload.put("recordType", "INSTANCE");
		okapi.post("/data-export/export", mapper.writeValueAsString(tPayload));

		// WAIT FOR DONE
		List<Map<String,String>> exportedFiles = null;
		while ( exportedFiles == null ) {
			Thread.sleep(500);
			List<Map<String, Object>> executions =
					okapi.queryAsList("/data-export/job-executions", String.format("id==%s", jobExecutionId));
			if (executions.size() != 1) {
				System.out.println("Expected exactly one matching execution. Found "+executions.size());
				for (Map<String,Object> execution : executions) System.out.println(execution);
				return null;
			}
			String status = (String)executions.get(0).get("status");
			System.out.println(status);
			if (status.equals("COMPLETED") || status.equals("COMPLETED_WITH_ERRORS"))
				exportedFiles = (List<Map<String, String>>) executions.get(0).get("exportedFiles");
		}

		// DOWNLOAD MARC
		List<MarcRecord> records = new ArrayList<>();
		for (Map<String,String> file : exportedFiles) {
			String exportUrl = String.format("/data-export/job-executions/%s/download/%s",
					jobExecutionId, file.get("fileId"));
			String fResponse = okapi.query(exportUrl);
			String fLink = (String) mapper.readValue(fResponse, Map.class).get("link");
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			try (InputStream is = (new URL(fLink)).openStream()) {
				byte[] chunk = new byte[4096];
				int n;
				while ( (n = is.read(chunk)) > 0 ) { os.write(chunk, 0, n); }
			}
			os.flush();
			byte[] marc = os.toByteArray();
			records.addAll(MarcRecord.readMarc21File(MarcRecord.RecordType.BIBLIOGRAPHIC, marc));
		}

		return records;
	}

	static ObjectMapper mapper = new ObjectMapper();
	static { mapper.enable(SerializationFeature.INDENT_OUTPUT); }
}
