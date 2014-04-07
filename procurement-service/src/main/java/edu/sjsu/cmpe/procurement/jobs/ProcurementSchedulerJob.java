package edu.sjsu.cmpe.procurement.jobs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.spinscale.dropwizard.jobs.Job;
import de.spinscale.dropwizard.jobs.annotations.Every;
import edu.sjsu.cmpe.procurement.ProcurementService;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

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
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;
/**
 * This job will run at every 5 second.
 */
@Every("300s")
public class ProcurementSchedulerJob extends Job {
    private final Logger log = LoggerFactory.getLogger(getClass());

    
    
    @Override
    public void doJob() {
    	
    	ArrayList<Long> isbnList = new ArrayList<Long>();
    	try {
			isbnList = consumeMessages();
			if (!isbnList.isEmpty()) {
				PostMessagesToPublisher(isbnList);
				getMessagesFromPublisher();
			}
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
    		if (message == null) {
    			System.out.println("No new messages. Exiting due to timeout - " + waitUntil / 1000 + " sec");
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
    		
    		else {
				System.out.println("Unexpected message type: " + message.getClass());
			}
    		
    		
    	}
    	connection.close();
		System.out.println("Done");
    	return isbnList;
    }
    
    public void PostMessagesToPublisher(ArrayList<Long> longArray) {
		try {

			Client client = Client.create();
			WebResource webResource = client
					.resource("http://54.215.133.131:9000/orders");

			String input = "{\"id\":\"84067\",\"order_book_isbns\":" + longArray + "}";
			System.out.println("$$$$$$$$$$$$$$$ LIST $$$$$$$$$$$$$$$$$$"+longArray);
			System.out.println(input);
			ClientResponse response = webResource.type("application/json").post(ClientResponse.class, input);

			if (response.getStatus() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
			}

			System.out.println("Output from Server .... \n");
			String output = response.getEntity(String.class);
			System.out.println(output);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
    
    public void getMessagesFromPublisher() {
		String output = "";
		String user = "admin";
		String password = "password";
		String host = "54.215.133.131";
		int port = 61613;

		try {

			Client client = Client.create();

			WebResource webResource = client
					.resource("http://54.215.133.131:9000/orders/84067");

			ClientResponse response = webResource.accept("application/json")
					.get(ClientResponse.class);

			if (response.getStatus() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
						+ response.getStatus());
			}
			output = response.getEntity(String.class);
			if (!output.isEmpty()) {

				System.out.println("Output from Server .... \n");
				System.out.println(output);

				String array1[] = output.split("\\[");
				 array1 = array1[1].split("\\}");
				for (int i = 0; i < array1.length; i++) {
					array1[i] = array1[i] + "}";
					if (i != 0)
						array1[i] = array1[i].substring(2);
				}

				String[] array2 = new String[array1.length-1];
				for (int j=0;j<array2.length;j++) {
					array2[j] = array1[j];
				}
				
				JSONArray nameArray = (JSONArray) JSONSerializer.toJSON(array2);

				log.info("NameArray size = {}", nameArray.size());
				if (nameArray != null) {
					for (Object jsonObject : nameArray) {
						JSONObject json = (JSONObject) JSONSerializer.toJSON(jsonObject);
						if (json != null) {
							log.info("Before getting ISBN");
							Integer isbn = 0;
							String title = "";
							String category = "";
							String coverImage ="";
							String data = "";

							if(json.get("isbn") != null){
								isbn = Integer.valueOf(json.get("isbn")	+ "");
								title = (String) json.get("title");
								category = (String) json.get("category");
								coverImage = (String) json.get("coverimage");

								data = isbn + ":\"" + title + "\":\""
										+ category + "\":\"" + coverImage + "\"";

							}

							log.info("Afer ISBN=" + isbn);

							if (category.equalsIgnoreCase("computer")) {
								String destination = "/topic/84067.book.computer";
								StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
								factory.setBrokerURI("tcp://" + host + ":"
										+ port);

								Connection connection = factory
										.createConnection(user, password);
								connection.start();
								Session session = connection.createSession(
										false, Session.AUTO_ACKNOWLEDGE);
								Destination dest = new StompJmsDestination(
										destination);
								MessageProducer producer = session
										.createProducer(dest);
								TextMessage msg = session
										.createTextMessage(data);
								msg.setLongProperty("id",
										System.currentTimeMillis());
								producer.send(msg);
								connection.close();
							}  else if (category.equalsIgnoreCase("management")) {
								String destination = "/topic/84067.book.management";
								StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
								factory.setBrokerURI("tcp://" + host + ":"
										+ port);

								Connection connection = factory
										.createConnection(user, password);
								connection.start();
								Session session = connection.createSession(
										false, Session.AUTO_ACKNOWLEDGE);
								Destination dest = new StompJmsDestination(
										destination);
								MessageProducer producer = session
										.createProducer(dest);
								TextMessage msg = session
										.createTextMessage(data);
								msg.setLongProperty("id",
										System.currentTimeMillis());
								producer.send(msg);
								connection.close();
							} else if (category.equalsIgnoreCase("comics")) {
								String destination = "/topic/84067.book.comics";
								StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
								factory.setBrokerURI("tcp://" + host + ":"
										+ port);

								Connection connection = factory
										.createConnection(user, password);
								connection.start();
								Session session = connection.createSession(
										false, Session.AUTO_ACKNOWLEDGE);
								Destination dest = new StompJmsDestination(
										destination);
								MessageProducer producer = session
										.createProducer(dest);
								TextMessage msg = session
										.createTextMessage(data);
								msg.setLongProperty("id",
										System.currentTimeMillis());
								producer.send(msg);
								connection.close();
							} else if (category
									.equalsIgnoreCase("selfimprovement")) {
								String destination = "/topic/84067.book.selfimprovement";
								StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
								factory.setBrokerURI("tcp://" + host + ":"
										+ port);

								Connection connection = factory
										.createConnection(user, password);
								connection.start();
								Session session = connection.createSession(
										false, Session.AUTO_ACKNOWLEDGE);
								Destination dest = new StompJmsDestination(
										destination);
								MessageProducer producer = session
										.createProducer(dest);
								TextMessage msg = session
										.createTextMessage(data);
								msg.setLongProperty("id",
										System.currentTimeMillis());
								producer.send(msg);
								connection.close();
							} 

						}
					}

				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
    
    
    
}
