package edu.cornell.library.integration.util;

import java.util.regex.Pattern;

/**
 * These are constants that are used with the
 * MARC to XML converter.
 * 
 */
public class MarcToXmlConstants {
    
    
    /** The range of non allowable characters in XML 1.0 (ASCII Control Characters) */
    public static final String WEIRD_CHARACTERS =
       "[\u0001-\u0008\u000b-\u000c\u000e-\u001f]";
    
    public static final Pattern WEIRD_CHARACTERS_PATTERN =
          Pattern.compile(WEIRD_CHARACTERS);
    
    public static final String LN = System.getProperty("line.separator");
    
    public static final String TMPDIR = "/tmp";      
    
    /** MARC-8 ANSEL ENCODING **/
    public static final String MARC_8_ENCODING = "MARC8";
    
    /** ISO5426 ENCODING **/
    public static  final String ISO5426_ENCODING = "ISO5426";

    /** ISO6937 ENCODING **/
    public static  final String ISO6937_ENCODING = "ISO6937";
    

}
