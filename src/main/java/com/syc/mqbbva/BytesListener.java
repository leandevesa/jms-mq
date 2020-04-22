package com.syc.mqbbva;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.jms.BytesMessage;
import javax.jms.Message;
import javax.jms.MessageListener;

public class BytesListener implements MessageListener {

	public void onMessage(Message message) {
        System.out.println("## entry onMessage");

        BytesMessage bytesMessage = (BytesMessage) message;
        byte[] bytesResponse;

        try {
            bytesResponse = new byte[(int)bytesMessage.getBodyLength()];
            bytesMessage.readBytes(bytesResponse);
            InputStream is = new ByteArrayInputStream(bytesResponse);
            BufferedReader buf = new BufferedReader(new InputStreamReader(is));
            String line = buf.readLine();
            System.out.println(line);
        } catch (Exception e) {
        
        }
		
		System.out.println("##exit onMessage"); // So we know when onMessage has finished execution
	}
}