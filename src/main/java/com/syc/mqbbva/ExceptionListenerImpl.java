package com.syc.mqbbva;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import org.apache.log4j.Logger;

public class ExceptionListenerImpl implements ExceptionListener {

    private final static Logger logger = Logger.getLogger(ExceptionListenerImpl.class);

    @Override
    public void onException(JMSException e) {
        logger.info("exception ocurred on connection", e);
    }
}
