package edu.cornell.library.integration.hathitrust;

import java.util.ArrayList;
import java.util.List;

public class Utilities {

	static List<String> identifyPrefixes(String prefixes_param) {
		List<String> prefixes = new ArrayList<>();
		if (prefixes_param == null) {
			prefixes.add("");
			return prefixes;
		}
		String[] args = prefixes_param.split(",");
		for (String arg : args) {
			if (arg.equalsIgnoreCase("None") || arg.isEmpty())
				prefixes.add("");
			else {
				String cleaned = arg.replaceAll("[^_A-Za-z0-9]", "");
				if (cleaned.isEmpty()) {
					System.out.printf("Skipping prefix \"%s\" due to no legal characters.\n", arg);
					continue;
				} else if ( ! cleaned.equals(arg) )
					System.out.printf("Prefix \"%s\" shortened to \"%s\" because only alphanumeric and _ are allowed.\n", arg, cleaned);
				prefixes.add(cleaned);
			}
		}
		return prefixes;
	}

}
