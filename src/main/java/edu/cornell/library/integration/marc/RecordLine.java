package edu.cornell.library.integration.marc;

import java.nio.charset.StandardCharsets;

/**
 * Information about one line in a MARC record's textual presentation: 
 * the text of {@link #line}, {@link #beginningOfLine}, {@link #endOfLine},
 * {@link #position} (character positions inside the record), 
 * {@link #positionInLine}. A line practically represents a MARC field.
 * 
 * Examples of textual representation of lines:
 * <pre>
 * LEADER 00714cam a2200205 a 4500 // a Leader
 * 001 12883376                    // a control field
 * 020   $a0786808772              // a data field
 * 100 1 $aChabon, Michael.        // a data field with indicator
 * </pre>
 * 
 * @author kiru
 */
class RecordLine {
	
   public static final String LN = System.getProperty("line.separator"); 

	/** The character position of the begining of the line */
	private int beginningOfLine;
	
	/** The character position of the end of the line */
	private int endOfLine;
	
	/** The String representation of the line */
	private String line;
	
	/** The position of the error character in the record */
	private int position;

	/** The position of the error character in the line */
	private int positionInLine = -2;
	
	/**
	 * Create a new RecordLine object
	 * @param recordString The string representation of a MARC record
	 * @param position The prosition of a invalid character inside record
	 */
	public RecordLine(String recordString, int position) {
		this.position = position;
		endOfLine = recordString.indexOf("\n", position);
		beginningOfLine = recordString.substring(0, endOfLine).lastIndexOf("\n") + 1;
		if(beginningOfLine == -1) {
			beginningOfLine = 0;
		}
		line = recordString.substring(beginningOfLine, endOfLine);
	}

	/**
	 * Get the character position of the beginning of the line
	 * @return {@link #beginningOfLine}
	 */
	public int getBeginningOfLine() {
		return beginningOfLine;
	}

	/**
	 * Get the character position of the end of the line
	 * @return {@link #endOfLine}
	 */
	public int getEndOfLine() {
		return endOfLine;
	}

	/**
	 * Get the string representation of the line
	 * @return {@link #line}
	 */
	public String getLine() {
		return line;
	}
	
	/**
	 * Get the position of the invalid character inside the record
	 * @return {@link #position}
	 */
	public int getPosition() {
		return position;
	}
	
	/**
	 * Get the position of the invalid character inside the line
	 * @return {@link #positionInLine}
	 */
	public int getPositionInLine() {
		if(positionInLine == -2) {
			positionInLine = position - beginningOfLine;
		}
		return positionInLine;
	}
	
	/**
	 * Get the invalid character
	 * @return
	 */
	public String getInvalidChar() {
		return line.substring(getPositionInLine(), getPositionInLine()+1);
	}

	/**
	 * Get the hexa representation of the invalid character 
	 * @return
	 */
	public String getInvalidCharHexa() {
		return charToHexa(getInvalidChar().charAt(0));
	}
	
	/**
	 * Show the location of the invalid character. An example:
	 * <pre>
	 * LEADER 01502ccm a2200361 4500
	 * ------------------------^ ('\u000E')
	 * </pre> 
	 * @return
	 */
	public String getErrorLocation() {
		StringBuffer invalidCharLocator = new StringBuffer();
		invalidCharLocator.append(line).append(LN);
		for(int pos = beginningOfLine; pos < position; pos++) {
			invalidCharLocator.append('-');
		}
		invalidCharLocator.append("^ (");
		invalidCharLocator.append(getInvalidCharHexa()).append(")");
		return invalidCharLocator.toString();
	}
	
	/**
    * Return the UTF-8 representation ('\\uxxxx') of a character
    * @param a
    * @return
    */
   public static String charToHexa(char a) {
      byte[] bytes = String.valueOf(a).getBytes(StandardCharsets.UTF_8);
      
      StringBuffer sb = new StringBuffer();
      sb.append("'\\u");
      if(1 == bytes.length){
         sb.append("00");
      }
      String hex;
      for (int j = 0; j < bytes.length; j++) {
         hex = Integer.toHexString(bytes[j] & 0xff).toUpperCase();
         if(1 == hex.length()) {
            sb.append('0');
         }
         sb.append(hex);
      }
      sb.append('\'');
      return sb.toString();
   }
}