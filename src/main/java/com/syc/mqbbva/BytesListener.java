package com.syc.mqbbva;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

import javax.jms.Message;
import javax.jms.MessageListener;

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
        
        logger.info("message received, version 0.2");
        logger.info(message.getClass().toString());

        try {
            JmsTextMessageImpl textMessage = (JmsTextMessageImpl) message;
            text = textMessage.getText();
            logger.info("body length: " + String.valueOf(text.length()));
            logger.info("text received: " + text);
            mid = textMessage.getJMSMessageID();
            logger.info("message id: " + mid);
        } catch (Exception e) {
            logger.error("failed parsing message", e);
            return;
        }

        try {
            mid = mid.replace("ID:", "");
            String filePath = this.outputPath + mid + ".txt";

            File outputFile = new File(filePath);
            if (!outputFile.exists()) {
                outputFile.getParentFile().mkdirs();
                outputFile.createNewFile();
                OutputStream outStream = new FileOutputStream(outputFile);
                outStream.write(text.getBytes());
                outStream.close();
            } else {
                logger.info(mid + " already existed");
            }
        } catch (Exception e) {
            logger.error("failed writing output file", e);
            return;
        }
		
		logger.info("message parsed ok!");
	}
}