package com.syc.mqbbva;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;

import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import com.ibm.msg.client.wmq.WMQConstants;

import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

	// Create variables for the connection to MQ
	private static String HOST; // Host name or IP address
	private static int PORT; // Listener port for your queue manager
	private static String CHANNEL; // Channel name
	private static String QMGR; // Queue manager name
	private static String APP_USER; // User name that application uses to connect to MQ
	private static String APP_PASSWORD; // Password that the application uses to connect to 
	private static String QUEUE_NAME; // Queue that the application uses to put and get messages to and from
	private static String OUTPUT_PATH;

	private static Logger logger;

	public static void main(String[] args) {

		try (InputStream input = new FileInputStream("config.properties")) {

			Properties prop = new Properties();

            prop.load(input);

			System.setProperty("my.log", prop.getProperty("log.path"));
			
			logger = Logger.getLogger(Application.class);

			logger.info("retrieving host");
			HOST = prop.getProperty("mq.host");

			logger.info("retrieving port");
			PORT = Integer.parseInt(prop.getProperty("mq.port"));
			
			logger.info("retrieving channel");
			CHANNEL = prop.getProperty("mq.channel");

			logger.info("retrieving qmgr");
			QMGR = prop.getProperty("mq.qmgr");

			logger.info("retrieving app user");
			APP_USER = prop.getProperty("mq.app.user");

			logger.info("retrieving app password");
			APP_PASSWORD = prop.getProperty("mq.app.password");

			logger.info("retrieving queue name");
			QUEUE_NAME = prop.getProperty("mq.queue.name");

			logger.info("retrieving outputh path");
			OUTPUT_PATH = prop.getProperty("message.output.path");

        } catch (Exception e) {
			logger.error(e.getMessage());
			return;
		}
		
		logger.info("booting...");

		SpringApplication.run(Application.class, args);

		logger.info("boot ok");

		// Declare JMS 2.0 objects
		JMSContext context;
		Destination destination; // The destination will be a queue, but could also be a topic 
		JMSConsumer consumer;

		logger.info("creating jms connection factory");
		
		JmsConnectionFactory connectionFactory = createJMSConnectionFactory();

		logger.info("setting jms properties");

		setJMSProperties(connectionFactory);

		logger.info("MQ Test: Connecting to " + HOST + ", Port " + PORT + ", Channel " + CHANNEL
		+ ", Connecting to " + QUEUE_NAME);

		try {
			logger.info("creating context");

			context = connectionFactory.createContext(); // This is connection + session. The connection is started by default

			logger.info("creating queue");
			
			destination = context.createQueue("queue:///" + QUEUE_NAME); // Set the producer and consumer destination to be the same... not true in general
			
			logger.info("creating consumer");

			consumer = context.createConsumer(destination); // associate consumer with the queue we put messages onto
			
			logger.info("creating bytes listener");

			/************IMPORTANT PART******************************/
			MessageListener ml = new BytesListener(OUTPUT_PATH); // Creates a listener object
			
			logger.info("setting listener");
			
			consumer.setMessageListener(ml); // Associates listener object with the consumer
			
			logger.info("The message listener is running."); // (Because the connection is started by default)
			
			// The messaging system is now set up
			/********************************************************/

			userInterface(context, connectionFactory, destination);
			
		} catch (Exception e) {
			// if there is an associated linked exception, print it. Otherwise print the stack trace
			if (e instanceof JMSException) { 
				JMSException jmse = (JMSException) e;
				if (jmse.getLinkedException() != null) { 
					logger.error("!! JMS exception thrown in application main method !!");
					logger.error(jmse.getLinkedException());
				}
				else {
					jmse.printStackTrace();
				}
			} else {
				logger.error("!! Failure in application main method !!");
				e.printStackTrace();
			}
			logger.error(e.getMessage());
		}
	}
	
	private static JmsConnectionFactory createJMSConnectionFactory() {
		JmsFactoryFactory ff;
		JmsConnectionFactory cf;
		try {
			ff = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);
			cf = ff.createConnectionFactory();
		} catch (JMSException jmse) {
			logger.error("JMS Exception when trying to create connection factory!");
			if (jmse.getLinkedException() != null){ // if there is an associated linked exception, print it. Otherwise print the stack trace
				logger.error(((JMSException) jmse).getLinkedException());
			} else {jmse.printStackTrace();}
			cf = null;
		} catch (Exception e) {
			logger.error("Exception trying to create connection factory");
			logger.error(e.getMessage());
			cf = null;
		}
		return cf;
	}

	private static void setJMSProperties(JmsConnectionFactory cf) {
		try {
			cf.setStringProperty(WMQConstants.WMQ_HOST_NAME, HOST);
			cf.setIntProperty(WMQConstants.WMQ_PORT, PORT);
			cf.setStringProperty(WMQConstants.WMQ_CHANNEL, CHANNEL);
			cf.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_CLIENT);
			cf.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER, QMGR);
			cf.setStringProperty(WMQConstants.WMQ_APPLICATIONNAME, "JmsPutGet (JMS)");
			cf.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, true);
			cf.setStringProperty(WMQConstants.USERID, APP_USER);
			cf.setStringProperty(WMQConstants.PASSWORD, APP_PASSWORD);
		} catch (JMSException jmse) {
			logger.error("JMS Exception when trying to set JMS properties!");
			if (jmse.getLinkedException() != null){ // if there is an associated linked exception, print it. Otherwise print the stack trace
				logger.error(((JMSException) jmse).getLinkedException());
			} else {jmse.printStackTrace();}
			logger.error(jmse.getMessage());
		} catch (Exception e) {
			logger.error("JMS Exception when trying to set JMS properties! - 2");
			logger.error(e.getMessage());
		}
		return;
	}

	public static void userInterface(JMSContext context, JmsConnectionFactory connectionFactory, Destination destination) {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		Boolean exit = false;
		while (!exit) {
			String command;
			try {
				System.out.print("Ready : ");
				command = br.readLine(); // Takes command line input
				command = command.toLowerCase();

				switch (command) {
					case "start": case "restart":
						context.start(); // Starting the context also starts the message listener
						System.out.println("--Message Listener started.");
						break;
					case "stop":
						context.stop(); // Stopping the context also stops the message listener
						System.out.println("--Message Listener stopped.");
						break;
					case "sendfile":
						sendATextFile(connectionFactory, destination);
						System.out.println("--Sent text file message.");
						break;
					case "exit":
						context.close(); // Also stops the context
						System.out.println("bye...");
						exit = true;
						break;
					default:
						System.out.println("Help: valid commands are start/restart, stop, send and exit");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.exit(0);
	}

	public static void sendATextFile(JmsConnectionFactory connectionFactory, Destination destination) {
		try {
			JMSContext producerContext = connectionFactory.createContext();
			JMSProducer producer = producerContext.createProducer();
			byte[] fileBytes = Files.readAllBytes(Paths.get("file-test.txt"));
			BytesMessage m = producerContext.createBytesMessage();
			m.writeBytes(fileBytes);
			producer.send(destination, m);
			producerContext.close();
		} catch (Exception e) {
			System.out.println("Exception when trying to send a text message!");
			// if there is an associated linked exception, print it. Otherwise print the stack trace
			if (e instanceof JMSException) { 
				JMSException jmse = (JMSException) e;
				if (jmse.getLinkedException() != null) { 
					System.out.println(jmse.getLinkedException());
				}
				else {
					jmse.printStackTrace();
				}
			} else {
				e.printStackTrace();
			}
		}
	}

}
