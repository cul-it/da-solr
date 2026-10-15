package edu.cornell.library.integration.authority.mads;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum RecordStatus {
	NEW("new"), REVISED("revised"), DEPRECATED("deprecated"), UPDATED("updated");

	private final String display;
	private RecordStatus( String display ) {
		this.display = display;
	}
	public String getDisplay() {
		return this.display;
	}

	private static final Map<Integer,RecordStatus> _byOrdinal =
			Stream.of(RecordStatus.values()).collect(Collectors.toMap(s -> s.ordinal(), s -> s));

	public static RecordStatus byOrdinal( int ordinal ) {
		return _byOrdinal.get(ordinal);
	}

	public static RecordStatus byName(String recordStatus) {
		for (RecordStatus status : RecordStatus.values()) {
			if (status.display.equalsIgnoreCase(recordStatus) || status.name().equalsIgnoreCase(recordStatus)) {
				return status;
			}
		}

		return null;
		// throw new IllegalArgumentException("No constant with valid record status '" + recordStatus + "' found");
	}
}
