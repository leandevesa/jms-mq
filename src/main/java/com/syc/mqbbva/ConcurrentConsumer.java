package com.syc.mqbbva;

import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.MessageListener;
import org.apache.log4j.Logger;

public class ConcurrentConsumer implements Runnable {

    private String HOST; // Host name or IP address
    private int PORT; // Listener port for your queue manager
    private String CHANNEL; // Channel name
    private String QMGR; // Queue manager name
    private String APP_USER; // User name that application uses to connect to MQ
    private String APP_PASSWORD; // Password that the application uses to connect to
    private String QUEUE_NAME; // Queue that the application uses to put and get messages to and from
    private String OUTPUT_PATH;
    private Logger logger;
    private Long TID;

    private boolean process = true;

    public ConcurrentConsumer(String host, int port, String channel, String qmgr, String appUser, String appPassword, String queueName,
                              String outputPath, long tid) {

        this.HOST = host;
        this.PORT = port;
        this.CHANNEL = channel;
        this.QMGR = qmgr;
        this.APP_USER = appUser;
        this.APP_PASSWORD = appPassword;
        this.QUEUE_NAME = queueName;
        this.OUTPUT_PATH = outputPath;
        this.TID = tid;

        logger = Logger.getLogger(ConcurrentConsumer.class);
    }

    @Override
    public void run() {

        log(TID);

        // Declare JMS 2.0 objects
        JMSContext context;
        Destination destination; // The destination will be a queue, but could also be a topic 
        JMSConsumer consumer;

        log("creating jms connection factory");

        JmsConnectionFactory connectionFactory = createJMSConnectionFactory();

        log("setting jms properties");

        setJMSProperties(connectionFactory);

        log("MQ Test: Connecting to " + HOST + ", Port " + PORT + ", Channel " + CHANNEL
            + ", Connecting to " + QUEUE_NAME);

        try {

            log("creating context");

            context = connectionFactory.createContext(); // This is connection + session. The connection is started by default

            log("creating queue");

            destination = context.createQueue("queue:///" + QUEUE_NAME); // Set the producer and consumer destination to be the same... not true in general

            log("creating consumer");

            consumer = context.createConsumer(destination); // associate consumer with the queue we put messages onto

            log("creating bytes listener");

            MessageListener ml = new BytesListener(OUTPUT_PATH); // Creates a listener object

            log("setting listener");

            consumer.setMessageListener(ml); // Associates listener object with the consumer

            log("The message listener is running."); // (Because the connection is started by default)

            while (process) {
                Thread.sleep(500);
            }

            log("process ended ok!");

        } catch (Exception e) {
            // if there is an associated linked exception, print it. Otherwise print the stack trace
            if (e instanceof JMSException) {
                JMSException jmse = (JMSException) e;
                if (jmse.getLinkedException() != null) {
                    log("!! JMS exception thrown in application main method !!", e);
                    log("detail", jmse.getLinkedException());
                }
                else {
                    jmse.printStackTrace();
                }
            } else {
                log("!! Failure in application main method !!", e);
            }
            log("starting context failed", e);
        }
    }

    public void stop() {
        logger.info("stopping: " + TID);
        this.process = false;
        logger.info("stopped: " + TID);
    }

    private JmsConnectionFactory createJMSConnectionFactory() {
        JmsFactoryFactory ff;
        JmsConnectionFactory cf;
        try {
            ff = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);
            cf = ff.createConnectionFactory();
        } catch (JMSException jmse) {
            log("JMS Exception when trying to create connection factory!", jmse);
            if (jmse.getLinkedException() != null){ // if there is an associated linked exception, print it. Otherwise print the stack trace
                log("detail 2", ((JMSException) jmse).getLinkedException());
            } else {jmse.printStackTrace();}
            cf = null;
        } catch (Exception e) {
            log("Exception trying to create connection factory", e);
            cf = null;
        }
        return cf;
    }

    private void setJMSProperties(JmsConnectionFactory cf) {
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
            log("JMS Exception when trying to set JMS properties!", jmse);
            if (jmse.getLinkedException() != null){ // if there is an associated linked exception, print it. Otherwise print the stack trace
                log("detail 3", ((JMSException) jmse).getLinkedException());
            } else {jmse.printStackTrace();}
            log("detail 4", jmse);
        } catch (Exception e) {
            log("JMS Exception when trying to set JMS properties! - 2", e);
        }
    }

    private void log(String msg) {
        logger.info(TID + " - " + msg);
    }

    private void log(Long msg) {
        logger.info(TID + " - " + msg);
    }

    private void log(String msg, Exception e) {
        logger.info(TID + " - " + msg);
    }

    private void log(Long msg, Exception e) {
        logger.info(TID + " - " + msg);
    }
}
