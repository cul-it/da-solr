package edu.cornell.library.integration.utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CSVReader {

	final private FileReader r;
	final private BufferedReader reader;
	final private List<String> columnNames;
	public CSVReader(String filename) throws IOException {
		this.r = new FileReader(new File(filename));
		this.reader = new BufferedReader(this.r);
		String line = this.reader.readLine();
		if ( line == null )
			throw new IOException("Error reading data from file");
		this.columnNames = csvSplit(line);
		System.out.println(String.join("**", this.columnNames));
	}

	public Map<String,String> readLine() throws IOException {
		String line = this.reader.readLine();
		if ( line == null ) return null;
		List<String> values = csvSplit(line);
		Map<String,String> row = new HashMap<>();
		for ( int i = 0 ; i < this.columnNames.size() ; i++ )
			row.put(this.columnNames.get(i), trimQuotes(values.get(i)));
		return row;
	}

	private static String trimQuotes(String s) {
		if ( s.startsWith("\"") && s.endsWith("\"") )
			return s.substring(1, s.length()-1).replaceAll("\"\"", "\"");
		return s.replaceAll("\"\"", "\"");
	}

	private static List<String> csvSplit(String line) {

		List<String> values = new ArrayList<>();

		int quotesFound = 0;
		int lastSplitPos = -1;
		for (int pos = 0; pos < line.length(); pos++) {
			switch (line.charAt(pos)) {
			case '"':
				quotesFound++;
				break;
			case ',':
				if (quotesFound % 2 != 0)
					break;
				values.add(line.substring(lastSplitPos+1,pos).replaceAll("\"\"","\""));
				lastSplitPos = pos;
			}
		}

		return values;
	}

}