package edu.cornell.library.integration.metadata.support;

import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

import edu.cornell.library.integration.folio.FolioClient;
import edu.cornell.library.integration.folio.ReferenceData;

public class SupportReferenceData {

	public static ReferenceData contributorTypes;
	public static ReferenceData contributorNameTypes;
	public static ReferenceData identifierTypes;
	public static ReferenceData instanceNoteTypes;
	public static ReferenceData instanceStatuses;
	public static ReferenceData locations;
	public static void initialize( FolioClient folio) throws IOException {
		contributorTypes = new ReferenceData( folio, "/contributor-types","name");
		contributorNameTypes = new ReferenceData( folio, "/contributor-name-types","ordering");
		identifierTypes = new ReferenceData( folio, "/identifier-types","name");
		instanceNoteTypes = new ReferenceData( folio, "/instance-note-types","name");
		instanceStatuses = new ReferenceData( folio, "/instance-statuses","code");
		locations = new ReferenceData( folio,"/locations","code");
	}
	public static void initializeContributorTypes(String filename) throws IOException {
		contributorTypes = new ReferenceData( loadResourceFile(filename), "name");
	}
	public static void initializeContributorNameTypes(String filename) throws IOException {
		contributorNameTypes = new ReferenceData( loadResourceFile(filename), "ordering");
	}
	public static void initializeIdentifierTypes(String filename) throws IOException {
		identifierTypes = new ReferenceData( loadResourceFile(filename), "name");
	}
	public static void initializeInstanceNoteTypes(String filename) throws IOException {
		instanceNoteTypes = new ReferenceData( loadResourceFile(filename), "name");
	}
	public static void initializeInstanceStatuses(String filename) throws IOException {
		instanceStatuses = new ReferenceData( loadResourceFile(filename), "code");
	}
	public static void initializeLocations(String filename) throws IOException {
		locations = new ReferenceData( loadResourceFile(filename), "code");
	}

	public static String loadResourceFile(String filename) throws IOException {
		try ( InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
				Scanner s = new Scanner(is,"UTF-8")) {
			return s.useDelimiter("\\A").next();
		}
	}
}
