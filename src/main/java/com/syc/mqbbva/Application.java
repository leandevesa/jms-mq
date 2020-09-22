package com.syc.mqbbva;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
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
import javax.jms.Message;

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

			System.out.println("App init");

			Properties prop = new Properties();

			System.out.println("Reading props file");

			prop.load(input);
			
			System.out.println("Setting log env");

			System.setProperty("my.log", prop.getProperty("log.path"));
			
			logger = Logger.getLogger(Application.class);

			logger.info("version 0.2 running");

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
			System.out.println(e); // TODO: Log this
			if (logger != null) logger.error("error starting app", e);
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

			if (args.length > 0 && "hold".equals(args[0])) {
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				br.readLine();
			}

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

			logger.info("args len: " + String.valueOf(args.length));
			if (args.length > 0) logger.info(args[0]);

			if (args.length > 0 && "ui".equals(args[0])) {
				userInterface(context, connectionFactory, destination);
			} else {
				System.out.println("press any key to exit");
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				br.readLine();
			}
			
		} catch (Exception e) {
			// if there is an associated linked exception, print it. Otherwise print the stack trace
			if (e instanceof JMSException) { 
				JMSException jmse = (JMSException) e;
				if (jmse.getLinkedException() != null) { 
					logger.error("!! JMS exception thrown in application main method !!", e);
					logger.error("detail", jmse.getLinkedException());
				}
				else {
					jmse.printStackTrace();
				}
			} else {
				logger.error("!! Failure in application main method !!", e);
			}
			logger.error("starting context failed", e);
		}
	}
	
	private static JmsConnectionFactory createJMSConnectionFactory() {
		JmsFactoryFactory ff;
		JmsConnectionFactory cf;
		try {
			ff = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);
			cf = ff.createConnectionFactory();
		} catch (JMSException jmse) {
			logger.error("JMS Exception when trying to create connection factory!", jmse);
			if (jmse.getLinkedException() != null){ // if there is an associated linked exception, print it. Otherwise print the stack trace
				logger.error("detail 2", ((JMSException) jmse).getLinkedException());
			} else {jmse.printStackTrace();}
			cf = null;
		} catch (Exception e) {
			logger.error("Exception trying to create connection factory", e);
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
			logger.error("JMS Exception when trying to set JMS properties!", jmse);
			if (jmse.getLinkedException() != null){ // if there is an associated linked exception, print it. Otherwise print the stack trace
				logger.error("detail 3", ((JMSException) jmse).getLinkedException());
			} else {jmse.printStackTrace();}
			logger.error("detail 4", jmse);
		} catch (Exception e) {
			logger.error("JMS Exception when trying to set JMS properties! - 2", e);
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
						sendFileAsTextMessage(connectionFactory, destination);
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

	public static void sendFileAsTextMessage(JmsConnectionFactory connectionFactory, Destination destination) {
		try {
			

			byte[] fileBytes = Files.readAllBytes(Paths.get("file-test.txt"));
			String payload = new String(fileBytes, StandardCharsets.US_ASCII);

			// Need a separate context to create and send the messages because they are received asynchronously
			JMSContext producerContext = connectionFactory.createContext();
			JMSProducer producer = producerContext.createProducer();
			Message m = producerContext.createTextMessage(payload);
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
