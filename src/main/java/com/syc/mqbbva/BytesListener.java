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
        
        logger.info("message received, version 0.3");
        logger.info(message.getClass().toString());

        try {
            JmsTextMessageImpl textMessage = (JmsTextMessageImpl) message;
            logger.info("casted ok");
            text = textMessage.getText();
            logger.info("body length: " + String.valueOf(text.length()));
            try {
                logger.info("trying to print text");
                logger.info("text received: " + text);
            } catch (Exception e) {
                logger.info("failed printing text", e);
            }
            mid = textMessage.getJMSMessageID();
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
}