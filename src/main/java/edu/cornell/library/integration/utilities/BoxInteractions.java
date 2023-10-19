package edu.cornell.library.integration.utilities;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.List;

import com.box.sdk.BoxConfig;
import com.box.sdk.BoxDeveloperEditionAPIConnection;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxFolder.Info;
import com.box.sdk.BoxItem;
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

	public static void uploadFileToBox( String keyFile, String boxFolder, String filename ) throws IOException {
		Reader reader = new FileReader(keyFile);
		BoxConfig boxConfig = BoxConfig.readFrom(reader);
		IAccessTokenCache tokenCache = new InMemoryLRUAccessTokenCache(100);
		BoxDeveloperEditionAPIConnection api = BoxDeveloperEditionAPIConnection.
				getAppEnterpriseConnection(boxConfig, tokenCache);

		BoxFolder rootFolder = BoxFolder.getRootFolder(api);
		BoxFolder outputFolder = null;
		for (BoxItem.Info itemInfo : rootFolder) {
			System.out.format("[%s] %s\n", itemInfo.getID(), itemInfo.getName());
			if (itemInfo.getName().equals(boxFolder))
				outputFolder = new BoxFolder(api,itemInfo.getID());
		}
		if (outputFolder == null)
			throw new IOException(String.format("Target folder on box, '%s' not found.\n", boxFolder));

		FileInputStream instream = new FileInputStream(filename);
		BoxFile.Info boxFile = outputFolder.uploadFile(instream, filename);
		instream.close();
		List<Info> path = boxFile.getPathCollection();
		for (Info i : path) System.out.println(i.getName()+" : "+i.getID());
		System.out.println("File uploaded.");
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
