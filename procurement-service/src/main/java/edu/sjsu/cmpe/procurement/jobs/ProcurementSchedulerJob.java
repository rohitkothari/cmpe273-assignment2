package edu.sjsu.cmpe.procurement.jobs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.spinscale.dropwizard.jobs.Job;
import de.spinscale.dropwizard.jobs.annotations.Every;
import edu.sjsu.cmpe.procurement.ProcurementService;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.ArrayList;
/**
 * This job will run at every 5 second.
 */
@Every("5s")
public class ProcurementSchedulerJob extends Job {
    private final Logger log = LoggerFactory.getLogger(getClass());

    
    
    @Override
    public void doJob() {
    	
    	ArrayList<Long> isbnList = new ArrayList<Long>();
    	try {
			isbnList = consumeMessages();
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
	String strResponse = ProcurementService.jerseyClient.resource(
		"http://ip.jsontest.com/").get(String.class);
	log.debug("Response from jsontest.com: {}", strResponse);
    }
    
    public ArrayList<Long> consumeMessages() throws JMSException {
    	String apolloUser = "admin";
    	String apolloPassword = "password";
    	String apolloHost = "54.215.133.131";
    	int port = 61613;
    	String queueName = "/queue/84067.book.orders";
    	String brokerURI = "tcp://" + apolloHost + ":" + port;
    	ArrayList<Long> isbnList = new ArrayList<Long>();
    	int i=0;
    	
    	StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
    	factory.setBrokerURI(brokerURI);
    	
    	Connection connection = factory.createConnection(apolloUser,apolloPassword);
    	connection.start();
    	
    	Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    	Destination dest = new StompJmsDestination(queueName);

    	MessageConsumer consumer = session.createConsumer(dest);
    	System.out.println("Waiting for messages from " + queueName + "...");
    	long waitUntil = 5000;
    	
    	while(true) {
    		Message message = consumer.receive(waitUntil);
    		if(message == null)	{
    			break;
    		}
    		
    		if(message instanceof TextMessage)	{
    			String msgBody = ((TextMessage) message).getText();
    			System.out.println("Testing : Message body: " +msgBody);
    			String isbn[] = msgBody.split(":");
				Long isbnValue = Long.parseLong(isbn[1]);
				System.out.println("Testing : isbn value: "+isbnValue);
				isbnList.add(i, isbnValue);
				i++;
    		}
    		
    		else if (message == null) {
    			System.out.println("No new messages. Existing due to timeout - " + waitUntil / 1000 + " sec");
				break;
			}
    		
    		else {
				System.out
						.println("Unexpected message type: " + message.getClass());
			}
    		
    		
    	}
    	connection.close();
		System.out.println("Done");
    	return isbnList;
    }
    
    
}
