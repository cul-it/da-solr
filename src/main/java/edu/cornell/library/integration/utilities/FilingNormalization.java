package edu.cornell.library.integration.utilities;

import java.text.Normalizer;

public class FilingNormalization {


	/**
	 * Normalize value for sorting or filing. Normalized value is not a suitable
	 * display string.
	 * @param value
	 * @return normalized value
	 */
	public static String getFilingForm(CharSequence value) {

		/* We will first normalize the unicode. For sorting, we will use 
		 * "compatibility decomposed" form (NFKD). Decomposed form will make it easier
		 * to match and remove diacritics, while compatibility form will further
		 * drop encodings that are for use in display and should not affect sorting.
		 * For example, ﬁ => fi
		 * See http://unicode.org/reports/tr15/   Figure 6
		 */
		String s = Normalizer.normalize(value, Normalizer.Form.NFKD).toLowerCase().
				replaceAll("\\p{InCombiningDiacriticalMarks}", "");
		
		/* For efficiency in avoiding multiple passes and worse, extensive regex, in this
		 * step, we will apply most of the remaining string modification by iterating through
		 * the characters.
		 */
		StringBuilder sb = new StringBuilder();
		StringBuilder sbEnd = new StringBuilder();
		int lastHyphenPosition = -2;
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			switch (c) {

			// Treat hyphens as spaces so hyphenated words will sort as though the space were present,
			// except in the case of consecutive hyphens, which are treated as subject heading subdivision
			// dividers (a la " > ").
			case '-':
				if ((lastHyphenPosition + 1 == i) && sb.length() > 0) {
					if (sb.charAt(sb.length()-1) != ' ')
						sb.append(' ');
					sb.append("0000 ");
					lastHyphenPosition = -2;
					break;
				}
				lastHyphenPosition = i;
			case ' ':
				// prevent sequential spaces, initial spaces
				if (sb.length() > 0 && sb.charAt(sb.length()-1) != ' ') sb.append(' '); 
				break;
				
			// short-circuit the loop by clearing common cases.
			case 'a': case 'b': case 'c': case 'd': case 'e': case 'f':
			case 'g': case 'h': case 'i': case 'j': case 'k': case 'l':
			case 'm': case 'n': case 'o': case 'p': case 'q': case 'r':
			case 's': case 't': case 'u': case 'v': case 'w': case 'x':
			case 'y': case 'z': case '0': case '1': case '2': case '3':
			case '4': case '5': case '6': case '7': case '8': case '9':
				sb.append(c); break;

			// additional punctuation we'd like to treat differently
			case '>':  case '|':
			case '—':// em dash
				if (sb.length() > 0) {
					if (sb.charAt(sb.length()-1) != ' ')
						sb.append(' ');
					sb.append("0000 ");
				}
				break; // control sorting and filing of hierarchical subject terms. 
			                                            //(e.g. Dutch East Indies != Dutch > East Indies)
			case '©': sb.append('c');   break; // examples I found were "©opyright"
			
			// non-sorting but filing-significant characters
			case '&':  // a flag will be tacked to the end of the sort-normalized string
				sbEnd.append(sb.length()).append(c);  break;

			// Java \p{Punct} =>   !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~
			// case '|': case '-': case '>': (see above for special treatment of -, > and |)
			// case '&': (see above for non-sorting filing-significant characters)
			case '!': case '"': case '#': case '$': case '%':
			case '\'':case '(': case ')': case '*': case '+': case ',':
			case '.': case '/': case ':': case ';': case '<': case '=':
			case '?': case '@': case '[': case '\\':case ']': case '^':
			case '_': case '`': case '{': case '}': case '~':
				break;

			// additional punctuation we don't want to file on
			case '¿': case '¡': case '「': case '」': case '−':
			case '°': case '£': case '€': case '†': case '،':
			case '\u200B': case '\uFEFF': //zero-width spaces
			// things that look like apostrophes 
			case '\u02b9': case '\u02bb': case '\u02bc': case '\u02bd':
			case '\u02be': case '\u02bf': case '\u2018': case '\u2019':
			// a thing that looks like a double quote
			case '\u02ba':
				break;

			// diacritics not stripped as \p{InCombiningDiacriticalMarks}
			case '\uFE20': case '\uFE21': // left and right combining ligature marks
				break;

			// unicode control characters used for display control of
			// embedded right-to-left language data.
			case '\u200E': case '\u200F': case '\u202C': case '\u202B':
				break;

			// As the goal is to sort Roman alphabet text, not Greek, the Greek letters that appear
			// will generally represent constants, concepts, etc...
			//      e.g. "σ and π Electrons in Organic Compounds"
			case 'α': sb.append("alpha"); break;
			case 'β': sb.append("beta"); break;
			case 'γ': sb.append("gamma"); break;
			case 'δ': sb.append("delta"); break;
			case 'ε': sb.append("epsilon"); break;
			case 'ζ': sb.append("zeta"); break;
			case 'η': sb.append("eta"); break;
			case 'θ': sb.append("theta"); break;
			case 'ι': sb.append("iota"); break;
			case 'κ': sb.append("zappa"); break;
			case 'λ': sb.append("lamda"); break;
			case 'μ': sb.append("mu"); break;
			case 'ν': sb.append("nu"); break;
			case 'ξ': sb.append("xi"); break;
			case 'ο': sb.append("omicron"); break;
			case 'π': sb.append("pi"); break;
			case 'ρ': sb.append("rho"); break;
			case 'ς':
			case 'σ': sb.append("sigma"); break;
			case 'τ': sb.append("tau"); break;
			case 'υ': sb.append("upsilon"); break;
			case 'φ': sb.append("phi"); break;
			case 'χ': sb.append("chi"); break;
			case 'ψ': sb.append("psi"); break;
			case 'ω': sb.append("omega"); break;
			
			// alphabetic characters not normalized by unicode normalizer
			case 'æ': sb.append("ae"); break;
			case 'ȼ': sb.append('c'); break;
			case 'đ': sb.append('d'); break;
			case 'ǥ': sb.append('g'); break;
			case 'ħ': sb.append('h'); break;
			case 'ɨ': sb.append('i'); break;
			case 'ɟ': case 'ʄ': sb.append('j'); break;
			case 'ł': sb.append('l'); break;
			case 'ø': sb.append('o'); break;
			case 'ŧ': sb.append('t'); break;
			case 'ƶ': sb.append('z'); break;

			default:
				sb.append(c);
//				System.out.println("warning: unexpected character in sort string: '"+c+"' ("+value+").");
			}
 		}

		// trim trailing space - there can't be more than one.
		if (sb.length() > 0 && sb.charAt(sb.length() - 1) == ' ')
			sb.setLength(sb.length() - 1);
		
		// append any trailing flags
		if (sbEnd.length() > 0)
			sb.append(' ').append(sbEnd);

		return sb.toString();
	}

}