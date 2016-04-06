package edu.cornell.library.integration.indexer.fieldMaker;

/**
 * Define a set of indicator requirements to be used by the StandardMARCFieldMaker
 * to pull appropriate records into the specified fields. If requirements are being
 * made on only one indicator, the first argument specifies which one. Character 
 * arguments identify the desired value, while Strings identify a set of good values.
 * 
 * e.g new IndicatorReq( 1, '1' );  // first indicator must be a '1'.
 *     new IndicatorReq('1',"012"); // first indicator must be a '1' AND second indicator one of '0','1','2'.
 */
public class IndicatorReq {
	
	/** There are no indicator requirements */
	public IndicatorReq( ) {
	}

	/** Indicator idno must have value c */
	public IndicatorReq( int indno, Character c ) {
		if (indno == 1)      equals1 = c;
		else if (indno == 2) equals2 = c;
		else throw new IllegalArgumentException("Indicator number (idno) must be 1 or 2.");
		
	}
	
	/** Indicator idno must have value in s */
	public IndicatorReq( int indno, String s ) {
		if (indno == 1)      in1 = s;
		else if (indno == 2) in2 = s;
		else throw new IllegalArgumentException("Indicator number (idno) must be 1 or 2.");
	}
	
	/** Indicator 1 must have value c1.
	 * Indicator 2 must have value c2. */
	public IndicatorReq( Character c1, Character c2 ) {
		equals1 = c1;
		equals2 = c2;
	}
	
	/** Indicator 1 must have value c1.
	 * Indicator 2 must have value in s2. */
	public IndicatorReq( Character c1, String s2 ) {
		equals1 = c1;
		in2 = s2;
	}

	/** Indicator 1 must have value in s1.
	 * Indicator 2 must have value c2. */
	public IndicatorReq( String s1, Character c2 ) {
		in1 = s1;
		equals2 = c2;
	}

	/** Indicator 1 must have value in s1.
	 * Indicator 2 must have value in s2. */
	public IndicatorReq( String s1, String s2 ) {
		in1 = s1;
		in2 = s2;
	}

	Character equals1 = null;
	Character equals2 = null;
	String in1 = null;
	String in2 = null;
	
}