package com.iot.kafka.producer;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

public class IoTDataEncoder implements Encoder<IoTData> {

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

}
