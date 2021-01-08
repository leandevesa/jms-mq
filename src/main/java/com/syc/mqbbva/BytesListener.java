package com.syc.mqbbva;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

import com.ibm.msg.client.jms.internal.JmsBytesMessageImpl;
import com.ibm.msg.client.jms.internal.JmsMessageImpl;
import com.ibm.msg.client.jms.internal.JmsTextMessageImpl;

import org.apache.log4j.Logger;

public class BytesListener implements MessageListener {

    private final String outputPath;
    private final static Logger logger = Logger.getLogger(BytesListener.class);

    public BytesListener(String outputPath) {
        this.outputPath = outputPath;
    }

    public void onMessage(Message message) {

        String mid;
        String text;
        
        logger.info("message received, version 0.4");
        
        String clazz = message.getClass().toString();
        
        logger.info(clazz);

        try {
            
            if (clazz.contains("JmsTextMessageImpl")) {
                text = getTextFromJmsTextMessageImpl(message);
            
            } else if (clazz.contains("JmsBytesMessageImpl")) {
                text = getTextFromJmsBytesMessageImpl(message);

            } else if (clazz.contains("JmsMessageImpl")) {
                text = getTextFromJmsMessageImpl(message);
            } else {

                logger.info("not supported message: " + clazz);
                throw new Exception("not supported message: " + clazz);
            }
            
            logger.info("body length: " + String.valueOf(text.length()));
            try {
                logger.info("trying to print text");
                logger.info("text received: " + text);
            } catch (Exception e) {
                logger.info("failed printing text", e);
            }
            mid = message.getJMSMessageID();
            logger.info("message id: " + mid);
        } catch (Exception e) {
            logger.info("failed parsing message", e);
            return;
        }

        try {
            logger.info("trying to create output file");

            mid = mid.replace("ID:", "");
            logger.info("replace ok");

            String filePath = this.outputPath + mid + ".txt";
            logger.info("filepath determined");

            File outputFile = new File(filePath);
            logger.info("file instance ok");

            if (!outputFile.exists()) {

                logger.info("file not exists");
                outputFile.getParentFile().mkdirs();
                logger.info("dirs created");

                outputFile.createNewFile();
                logger.info("output created");

                OutputStream outStream = new FileOutputStream(outputFile);
                logger.info("stream created");

                outStream.write(text.getBytes());
                logger.info("bytes written");

                outStream.close();
                logger.info("stream closed");
            } else {
                logger.info(mid + " already existed");
            }
        } catch (Exception e) {
            logger.info("failed writing output file", e);
            return;
        }

        logger.info("message parsed ok!");
    }

    private String getTextFromJmsTextMessageImpl(Message message) throws JMSException {
        logger.info("trying to desserialize textmessageimpl");

        JmsTextMessageImpl textMessage = (JmsTextMessageImpl) message;
        logger.info("casted ok 1");
        String text = textMessage.getText();

        return text;
    }
    
    private String getTextFromJmsBytesMessageImpl(Message message) throws JMSException {
        logger.info("trying to desserialize bytesmessageimpl");
        
        JmsBytesMessageImpl byteMessage = (JmsBytesMessageImpl) message;
        logger.info("casted ok 2");

        byteMessage.reset();
        logger.info("reset buffer ok");

        byte[] byteData = null;
        byteData = new byte[(int) byteMessage.getBodyLength()];

        logger.info("bytes read ok");

        byteMessage.readBytes(byteData);
        byteMessage.reset();
        String text =  new String(byteData);

        logger.info("string from bytes instancied ok");

        return text;
    }
    
    private String getTextFromJmsMessageImpl(Message message) throws JMSException {
        logger.info("trying to desserialize messageimpl");

        JmsMessageImpl messageImpl = (JmsMessageImpl) message;
        logger.info("casted ok 3");

        String text;

        try {
            logger.info("trying to get body as string");
            text = messageImpl.getBody(String.class);
        } catch (Exception e) {
            logger.info("failed to get body as string", e);
            logger.info(messageImpl.getBody(Object.class).getClass().toString());
            throw e;
        }

        return text;
    }
}