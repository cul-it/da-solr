package edu.cornell.library.integration.authority.activitystreams;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

enum MadsRecordStatus {
	NEW("new"), REVISED("revised"), DEPRECATED("deprecated");

	private final String display;
	private MadsRecordStatus( String display ) {
		this.display = display;
	}
	public String getDisplay() {
		return this.display;
	}

	private static Map<Integer,MadsRecordStatus> _byOrdinal =
			Stream.of(MadsRecordStatus.values()).collect(Collectors.toMap(s -> s.ordinal(), s -> s));

	public static MadsRecordStatus byOrdinal( int ordinal ) {
		return _byOrdinal.get(ordinal);
	}

	public static MadsRecordStatus byName(String recordStatus) {
		for (MadsRecordStatus status : MadsRecordStatus.values()) {
            if (status.display.equalsIgnoreCase(recordStatus) || status.name().equalsIgnoreCase(recordStatus)) {
                return status;
            }
        }
        throw new IllegalArgumentException("No constant with valid record status " + recordStatus + " found");
	}
}
