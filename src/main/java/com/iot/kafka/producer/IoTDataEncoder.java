package com.iot.kafka.producer;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public class IoTDataEncoder implements Serializer {

    private static final Logger logger = Logger.getLogger(IoTDataEncoder.class);
    private static ObjectMapper objectMapper = new ObjectMapper();

    public byte[] toBytes(IoTData iotEvent) {
        try {
            String msg = objectMapper.writeValueAsString(iotEvent);
            logger.info(msg);
            return msg.getBytes();
        } catch (JsonProcessingException e) {
            logger.error("Error in Serialization", e);
        }
        return null;
    }

    @Override
    public byte[] serialize(String s, Object o) {
        try {
            IoTData event = (IoTData) o;
            String msg = objectMapper.writeValueAsString(event);
            logger.info(msg);
            return msg.getBytes();
        } catch (JsonProcessingException e) {
            logger.error("Error in Serialization", e);
        }
        return null;
    }
}
