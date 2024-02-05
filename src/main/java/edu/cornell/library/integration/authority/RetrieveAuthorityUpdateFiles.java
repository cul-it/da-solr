package edu.cornell.library.integration.authority;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.utilities.Config; 

public class RetrieveAuthorityUpdateFiles {

	public static Map<String,String> ftpConfig = null;

	public static void checkLCAuthoritiesFTP( Config config ) throws SQLException {

		ftpConfig = config.getServerConfig("lcFtp");
		System.out.printf("[%s]=>[%s]\n", "Server",ftpConfig.get("Server"));

		FTPClient ftp = new FTPClient(); 
		try (Connection authdb = config.getDatabaseConnection("Authority");
				PreparedStatement stmt = authdb.prepareStatement(
						"INSERT INTO lcAuthorityFile( updateFile, postedDate, importedDate) values ( ?, ?, ? )")){ 

			ftp.connect(ftpConfig.get("Server"), 21); 
  
			int replyCode = ftp.getReplyCode(); 
			if ( ! FTPReply.isPositiveCompletion(replyCode) ) { 
				System.out.println("Operation failed. Server reply code: " + replyCode); 
				ftp.disconnect();
				return;
			} 
  
			if ( ! ftp.login(ftpConfig.get("User"), ftpConfig.get("Password")) ) {
				System.out.println("FTP login to LC Authorities failed.");
				ftp.disconnect(); 
			}
			System.out.println("Connected and logged into FTP.");
			ftp.setFileType(FTP.BINARY_FILE_TYPE); 
			ftp.enterLocalPassiveMode(); 

			String authDir = config.getAuthorityDataDirectory();
			System.out.printf("local auth directory: %s\n",authDir);
			importNewChanges( ftp, authdb, authDir, ftpConfig.get("NamesDir"), stmt);
			importNewChanges( ftp, authdb, authDir, ftpConfig.get("SubjectsDir"), stmt);
			ftp.disconnect();
		} 
		catch (UnknownHostException E) { 
			System.out.println("No such ftp server");
			return;
		} 
		catch (IOException e) { 
			System.out.println(e.getMessage()); 
			return;
		} 
	}

	private static void importNewChanges(
			FTPClient ftp, Connection authority, String authDir, String ftpDir, PreparedStatement stmt)
			throws IOException, SQLException {
		System.out.printf("Changing to remote folder: %s\n",ftpDir);
		if ( ! ftp.isConnected() )
			reconnectFtp(ftp,ftpDir);
		else
			ftp.changeWorkingDirectory(ftpDir);
		FTPFile[] ftpFiles = ftp.listFiles();
		for (FTPFile file : ftpFiles) {
			if ( ! file.isFile() ) continue;
			System.out.println(file);
			String name = file.getName();
			Matcher m = yy.matcher(name);
			if ( ! m.find() ) continue;
			stmt.setString(1, name);
			stmt.setTimestamp(2, new Timestamp(file.getTimestamp().getTimeInMillis() ));
			String year = ( m.group(1).startsWith("9") ) ? "19"+m.group(1) : "20"+m.group(1);
			String path = String.join(File.separator, authDir, year, name );
			File f = new File(path);
			if ( f.exists() ) {
//				stmt.setObject(3, null);
//				stmt.executeUpdate();
				continue;
			}
			System.out.println("Not present. Downloading.");
			File dir = new File(authDir+File.separator+year);
			if (! dir.exists()) dir.mkdir();

			Files.createFile(f.toPath());
			try (OutputStream out = new BufferedOutputStream(new FileOutputStream(f))) {
				if ( ! ftp.isConnected() )
					reconnectFtp(ftp,ftpDir);
				if ( ! ftp.retrieveFile(name, out) )
					throw new IOException(String.format("Error retrieving file '%s' from FTP.", name));
				out.flush();
			}
			System.out.println("Parsing MARC file.");
			Map<MarcRecord,String> records =
					LCAuthorityUpdateFile.readFile(f.toPath());
			System.out.printf("%d records retrieved from file %s.\n",records.size(),name);
			LCAuthorityUpdateFile.pushRecordsToDatabase(authority, records, name);

			stmt.setTimestamp(3, new Timestamp( new Date().getTime() ));
			stmt.executeUpdate();

		}
	}
	private static Pattern yy = Pattern.compile(".*(\\d\\d).\\d\\d$");

	private static void reconnectFtp(FTPClient ftp, String ftpDir) throws IOException {
		ftp.connect(ftpConfig.get("Server"), 21); 
		ftp.login(ftpConfig.get("User"), ftpConfig.get("Password"));
		ftp.changeWorkingDirectory(ftpDir);
	}
	public static void main(String[] args) throws SQLException {
		Config config = Config.loadConfig(Config.getRequiredArgsForDB("Authority"));
		checkLCAuthoritiesFTP(config);

	}

}
