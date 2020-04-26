package com.syc.mqbbva;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.jms.BytesMessage;
import javax.jms.Message;
import javax.jms.MessageListener;

import org.apache.log4j.Logger;

public class BytesListener implements MessageListener {

    private final String outputPath;
    private final static Logger logger = Logger.getLogger(BytesListener.class);
    
    public BytesListener(String outputPath) {
        this.outputPath = outputPath;
    }

	public void onMessage(Message message) {

        String mid;
        byte[] bytesResponse;
        
        logger.info("message received");
        logger.info(message.getClass().toString());

        try {
            BytesMessage bytesMessage = (BytesMessage) message;
            logger.info("body length: " + String.valueOf(bytesMessage.getBodyLength()));
            mid = bytesMessage.getJMSMessageID();
            logger.info("message id: " + mid);
            bytesResponse = new byte[(int)bytesMessage.getBodyLength()];
            bytesMessage.readBytes(bytesResponse);

        } catch (Exception e) {
            logger.error("failed parsing message");
            logger.error(e.getMessage());
            return;
        }

        try {

            Path path = Paths.get(this.outputPath);
            if (Files.notExists(path)) {
                Files.createDirectories(path);
            }

            mid = mid.replace("ID:", "");
            File outputFile = new File(this.outputPath + mid + ".txt");
            if (!outputFile.exists()) {
                outputFile.createNewFile();
                OutputStream outStream = new FileOutputStream(outputFile);
                outStream.write(bytesResponse);
                outStream.close();
            } else {
                logger.info(mid + " already existed");
            }
            /*
            InputStream is = new ByteArrayInputStream(bytesResponse);
            BufferedReader buf = new BufferedReader(new InputStreamReader(is));
            String line = buf.readLine();
            System.out.println(line);
            */
        } catch (Exception e) {
            logger.error("failed writing output file");
            logger.error(e.getMessage());
        }
		
		System.out.println("##exit onMessage"); // So we know when onMessage has finished execution
	}
}