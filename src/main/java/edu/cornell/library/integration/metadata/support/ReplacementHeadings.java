package edu.cornell.library.integration.metadata.support;

import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReplacementHeadings {

	public static ReplaceResults checkForHeadingReplacements(List<String> headingParts) {
		if (_replacements == null) throw new IllegalStateException("Replacement headings data not initialized.");
		List<String> headCopy = headingParts;
		List<String> filingParts = headCopy.stream().map(p -> getFilingForm(p)).toList();
		List<ReplaceHead> appliedReplacements = new ArrayList<>();

		for (int i = 0; i < filingParts.size(); i++) {
			if (_replacements.containsKey(filingParts.get(i))) {
				OverlayMatch match = lookForOverlay(
						_replacements.get(filingParts.get(i)), filingParts.subList(i, filingParts.size()), i > 0);
				if (match != null) {
					List<String> newParts = new ArrayList<>();
					newParts.addAll(headCopy.subList(0, i));
					for (String p : match.newParts) newParts.add(p);
					newParts.addAll(headCopy.subList(i+match.replaceHead.beforeForm.length, headCopy.size()));
					headCopy = newParts;
					filingParts = headCopy.stream().map(p -> getFilingForm(p)).toList();
					appliedReplacements.add(match.replaceHead);
				}
			}
		}
		if (appliedReplacements.isEmpty()) return null;
		return new ReplaceResults( headCopy, appliedReplacements );
	}

	public static void initialize(Connection headings) throws SQLException {
		_replacements = new HashMap<>();
		try (Statement stmt = headings.createStatement();
			 ResultSet rs = stmt.executeQuery("SELECT * FROM replacement_headings")) {
			while (rs.next()) {
				boolean isSubdiv = rs.getString(1).startsWith(">");
				String before = getFilingForm(rs.getString(1));
				String after = rs.getString(2);
				String[] beforeParts = before.split(" 0000 ");
				String top = beforeParts[0];
				if ( ! _replacements.containsKey(top))
					_replacements.put(top, new ArrayList<>());
				_replacements.get(top).add(new ReplaceHead(beforeParts,after,isSubdiv));
			}
			
		}
	}

	public static String toString(String unused) {
		StringBuilder b = new StringBuilder();
		for (String key : _replacements.keySet()) {
			b.append(key+"\n");
			for (ReplaceHead r : _replacements.get(key))
				b.append(r.toString()+"\n");
		}
		return b.toString();
	}

	public record ReplaceResults(
			List<String> afterHeading,
			List<ReplaceHead> appliedReplacements) {}

	public record ReplaceHead(
		String[] beforeForm,
		String afterForm,
		boolean isSubdiv
	) {
		public String toString() {
			return String.format(" [%s%s] [%s]" , (this.isSubdiv)?"> ":"",String.join(" > ", beforeForm),afterForm);
		}
	}

	private record OverlayMatch(
			String[] newParts,
			ReplaceHead replaceHead) {}

	private static OverlayMatch lookForOverlay(List<ReplaceHead> overlays, List<String> subdivs, boolean isSubdiv) {
		OVERLAY: for (ReplaceHead overlay : overlays) {
			if (isSubdiv != overlay.isSubdiv) continue;
			if (overlay.beforeForm.length == 1) {
				return new OverlayMatch(overlay.afterForm.split(" > ") ,overlay);
			} else {
				if (subdivs.size() < overlay.beforeForm.length)
					continue OVERLAY;
				for (int i = 1 ; i < overlay.beforeForm.length; i++)
					if ( ! overlay.beforeForm[i].equals(subdivs.get(i))) 
						continue OVERLAY;
				return new OverlayMatch(overlay.afterForm.split(" > ") , overlay);
			}
		}
		return null;
	}

	private static Map<String,List<ReplaceHead>> _replacements = null;

}
