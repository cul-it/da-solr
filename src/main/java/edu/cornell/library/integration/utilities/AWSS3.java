package edu.cornell.library.integration.utilities;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class AWSS3 {



	public static void putS3Object(Config config, String filename, String contents) {

		config.activateS3();

		S3Client client = S3Client.builder().region(Region.US_EAST_1).build();

		client.putObject(
				PutObjectRequest.builder().key(filename).bucket(config.getAwsS3Bucket()).build(),
				RequestBody.fromString(contents));
	}

}
