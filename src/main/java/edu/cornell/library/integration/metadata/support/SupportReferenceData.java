package edu.cornell.library.integration.metadata.support;

import java.io.IOException;

import edu.cornell.library.integration.folio.FolioClient;
import edu.cornell.library.integration.folio.ReferenceData;

public class SupportReferenceData {

	public static ReferenceData contributorTypes;
	public static ReferenceData contributorNameTypes;
	public static ReferenceData instanceNoteTypes;
	public static ReferenceData instanceStatuses;
	public static ReferenceData locations;
	public static void initialize( FolioClient folio) throws IOException {
		contributorTypes = new ReferenceData( folio, "/contributor-types","name");
		contributorNameTypes = new ReferenceData( folio, "/contributor-name-types","ordering");
		instanceNoteTypes = new ReferenceData( folio, "/instance-note-types","name");
		instanceStatuses = new ReferenceData( folio, "/instance-statuses","code");
		locations = new ReferenceData( folio,"/locations","code");
	}
	public static void initializeContributorTypes(String json) throws IOException {
		contributorTypes = new ReferenceData( json, "name");
	}
	public static void initializeContributorNameTypes(String json) throws IOException {
		contributorNameTypes = new ReferenceData( json, "ordering");
	}
	public static void initializeInstanceNoteTypes(String json) throws IOException {
		instanceNoteTypes = new ReferenceData( json, "name");
	}
	public static void initializeInstanceStatuses(String json) throws IOException {
		instanceStatuses = new ReferenceData( json, "code");
	}
	public static void initializeLocations(String json) throws IOException {
		locations = new ReferenceData( json, "code");
	}
}
