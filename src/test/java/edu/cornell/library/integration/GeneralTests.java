package edu.cornell.library.integration;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TreeSet;
import java.util.regex.Pattern;

public class GeneralTests {

	private static final Pattern uPlusHexPattern = Pattern.compile(".*[Uu]\\+\\p{XDigit}{4}.*");

	public GeneralTests() {
		String[] testStrings =
			{"String Matches U+00C0",
			 "String Doesn't match U+HI!",
			 "String U+0034ABC should match too.",
			 "u+abcd should this one?"};
		for (String test : testStrings) {
			Boolean matches = uPlusHexPattern.matcher(test).matches();
			System.out.println(test + ((matches)?": matches.":": doesn't match."));
		}
		TreeSet<Integer> primes = generatePrimeNumberList(200);
//		System.out.println(StringUtils.join(primes, ' '));
		/* First pass 1000 records - 169 primes = 831
		 *  2: 831 - 146 = 685
		 *  3: 685 - 125 = 560
		 *  4: 560 - 103 = 457
		 *  5; 457 - 89  = 368
		 *  6: 368 - 74  = 294
		 *  7: 294 - 63  = 231
		 *  8: 231 - 51  = 180
		 *  9: 180 - 42  = 138
		 * 10: 138 - 34  = 104
		 * 11: 104 - 28  = 76
		 * 12: 76  - 22  = 54
		 * 13: 54  - 17  = 37
		 * 14: 37 - 13   = 24
		 * 15: 24 - 10   = 14
		 * 16: 14 - 7    = 7
		 * 17: 7  - 5    = 2
		 * 18: 2  - 2    = 0
		*/
		System.out.println(new SimpleDateFormat("EEEEE").format(new Date()));
	}

	public static void main(String[] args) {
		new GeneralTests();
	}

	private TreeSet<Integer> generatePrimeNumberList(int number) {
		TreeSet<Integer> primeNumbers = new TreeSet<Integer>();
		int i = 0;
		MAIN: while (primeNumbers.size() < number) {
			int halfOfI = ++i/2;
			for (int j=2;j<=halfOfI;j++)
				if (i%j==0)
					continue MAIN;
			primeNumbers.add(i);
			if (primeNumbers.size()%10 == 0)
				System.out.println(primeNumbers.size()+": "+i);
		}
		return primeNumbers;
	}

}
