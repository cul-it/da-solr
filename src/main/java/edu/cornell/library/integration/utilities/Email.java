package edu.cornell.library.integration.utilities;


import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sesv2.SesV2Client;
import software.amazon.awssdk.services.sesv2.model.Body;
import software.amazon.awssdk.services.sesv2.model.Content;
import software.amazon.awssdk.services.sesv2.model.Destination;
import software.amazon.awssdk.services.sesv2.model.EmailContent;
import software.amazon.awssdk.services.sesv2.model.Message;
import software.amazon.awssdk.services.sesv2.model.SendEmailRequest;


public class Email {

	public static void sendSESHtmlMessage( String from, String to, String subject, String msg ) {

		SesV2Client ses = SesV2Client.builder().region(Region.US_EAST_1).build();

		Message message = Message.builder()
				.subject(Content.builder().data(subject).build())
				.body(Body.builder().html(Content.builder().data(msg).build()).build())
				.build();
		SendEmailRequest request = SendEmailRequest.builder()
				.fromEmailAddress(from)
				.destination(Destination.builder().toAddresses(to).build())
				.content(EmailContent.builder().simple(message).build())
				.build();
		ses.sendEmail(request);
	}

	public static void sendSESTextMessage( String from, String to, String subject, String msg ) {

		SesV2Client ses = SesV2Client.builder().region(Region.US_EAST_1).build();

		Message message = Message.builder()
				.subject(Content.builder().data(subject).build())
				.body(Body.builder().text(Content.builder().data(msg).build()).build())
				.build();
		SendEmailRequest request = SendEmailRequest.builder()
				.fromEmailAddress(from)
				.destination(Destination.builder().toAddresses(to).build())
				.content(EmailContent.builder().simple(message).build())
				.build();
		ses.sendEmail(request);
	}

}
