package edu.cornell.library.integration.metadata.support;

import java.io.IOException;

import edu.cornell.library.integration.folio.OkapiClient;
import edu.cornell.library.integration.folio.ReferenceData;

public class SupportReferenceData {

	public static ReferenceData contributorTypes;
	public static ReferenceData contributorNameTypes;
	public static ReferenceData instanceNoteTypes;
	public static void initialize( OkapiClient folio) throws IOException {
		contributorTypes = new ReferenceData( folio, "/contributor-types","name");
		contributorNameTypes = new ReferenceData( folio, "/contributor-name-types","ordering");
		instanceNoteTypes = new ReferenceData( folio, "/instance-note-types","name");
	}
}
