package com.syc.mqbbva;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
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

	private final static int MAX_THREADS = 200;
	private final static int THREAD_EXECUTION_WAIT_MS = 1000;
	private final static int THREADS_LIFETIME_MS = 1800000;

	private final static List<ConcurrentConsumer> concurrentConsumers = new ArrayList<>();

	public static void main(String[] args) throws InterruptedException {

		readPropertyFile();
		
		logger.info("booting...");

		SpringApplication.run(Application.class, args);

		logger.info("boot ok");

		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_THREADS);

		logger.info("executor ok");

		while (true) {

			logger.info("executing consumers, active: " + executor.getActiveCount());

			if (executor.getActiveCount() > 0) {
				logger.info("active threads running, waiting");
				Thread.sleep(25000);
				logger.info("now active: " + executor.getActiveCount());
			}

			for (int i = 0; i < MAX_THREADS; i++) {

				logger.info("trying to create: " + i);

				ConcurrentConsumer concurrentConsumer = new ConcurrentConsumer(HOST, PORT, CHANNEL, QMGR, APP_USER, APP_PASSWORD,
					QUEUE_NAME, OUTPUT_PATH, i);

				logger.info("consumer ok: " + i);

				concurrentConsumers.add(concurrentConsumer);

				logger.info("executing consumer: " + i);

				if (executor.getQueue() != null && executor.getQueue().size() > 0) {
					logger.info("WARNING, enqueing threads");
				}

				executor.execute(concurrentConsumer);

				logger.info("executed ok consumer: " + i);

				Thread.sleep(THREAD_EXECUTION_WAIT_MS);
			}

			logger.info("waiting for consumers lifetime");

			Thread.sleep(THREADS_LIFETIME_MS);

			logger.info("lifetime ended, stopping consumers");

			concurrentConsumers.forEach(ConcurrentConsumer::stop);

			logger.info("consumers stopped");

			Thread.sleep(5000);
		}
	}

	private static void readPropertyFile() {
		try (InputStream input = new FileInputStream("config.properties")) {

			System.out.println("App init");

			Properties prop = new Properties();

			System.out.println("Reading props file");

			prop.load(input);

			System.out.println("Setting log env");

			System.setProperty("my.log", prop.getProperty("log.path"));

			logger = Logger.getLogger(Application.class);

			logger.info("version 0.5 running");

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
			if (logger != null) logger.info("error starting app", e);
			return;
		}
	}
}
