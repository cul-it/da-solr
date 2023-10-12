package edu.cornell.library.integration.utilities;

import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;

import com.box.sdk.BoxConfig;
import com.box.sdk.BoxDeveloperEditionAPIConnection;
import com.box.sdk.BoxFile;
import com.box.sdk.IAccessTokenCache;
import com.box.sdk.InMemoryLRUAccessTokenCache;

public class BoxInteractions {

	public static String getBoxFileContents( String keyFile, String fileId, String fileName, int maxSize )
			throws IOException {
		Reader reader = new FileReader(keyFile);
		BoxConfig boxConfig = BoxConfig.readFrom(reader);
		IAccessTokenCache tokenCache = new InMemoryLRUAccessTokenCache(100);
		BoxDeveloperEditionAPIConnection api = BoxDeveloperEditionAPIConnection.
				getAppEnterpriseConnection(boxConfig, tokenCache);

		BoxFile file = new BoxFile(api, fileId);
		BoxFile.Info info = file.getInfo();
		if ( info.getSize() > maxSize ) {
			String msg = String.format("Requested file (%s) is larger than expected max size (%s).\n",
					fileName, humanReadableByteCountBin(maxSize));
			System.out.println(msg);
			throw new IllegalArgumentException(msg);
		}
		System.out.println("file_name "+info.getSize()+" bytes");
		ByteArrayOutputStream content = new ByteArrayOutputStream();
		file.download(content);
		return content.toString();
	}

	/**
	 * Source: https://programming.guide/java/formatting-byte-size-to-human-readable-format.html
	 * @param bytes
	 * @return file size as human readable string
	 */
	public static String humanReadableByteCountBin(long bytes) {
		long absB = bytes == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(bytes);
		if (absB < 1024) {
			return bytes + " B";
		}
		long value = absB;
		CharacterIterator ci = new StringCharacterIterator("KMGTPE");
		for (int i = 40; i >= 0 && absB > 0xfffccccccccccccL >> i; i -= 10) {
			value >>= 10;
			ci.next();
		}
		value *= Long.signum(bytes);
		return String.format("%.1f %ciB", value / 1024.0, ci.current());
	}
}
