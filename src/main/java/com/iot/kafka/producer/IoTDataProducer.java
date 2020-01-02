package com.iot.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.*;


public class IoTDataProducer {


    private static final Logger logger = Logger.getLogger(IoTDataProducer.class);

    public static void main(String[] args) throws Exception {

        //read config file
        Properties prop = PropertyFileReader.readPropertyFile();
        String zookeeper = prop.getProperty("com.iot.app.kafka.zookeeper");
        String brokerList = prop.getProperty("com.iot.app.kafka.brokerlist");
        String topic = prop.getProperty("com.iot.app.kafka.topic");

        logger.info("Using Zookeeper=" + zookeeper + " ,Broker-list=" + brokerList + " and topic " + topic);

        // set producer properties
        Properties properties = new Properties();

        properties.put("bootstrap.servers", brokerList);
        properties.put("client.id", "iot-producer");

        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.iot.kafka.producer.IoTDataEncoder");
        //generate event

        KafkaProducer<String,IoTData> producer = new KafkaProducer<String, IoTData>(properties);

//        Producer<String, IoTData> producer = new Producer<String, IoTData>(new ProducerConfig(properties));
        IoTDataProducer iotProducer = new IoTDataProducer();
        iotProducer.generateIoTEvent(producer,topic);
    }


    private void generateIoTEvent(KafkaProducer<String, IoTData> producer, String topic) throws InterruptedException {

        List<String> routeList = Arrays.asList(new String[]{"PTS_1", "PTS_2", "PTS_3", "PTS_4", "PTS_5"});
        List<String> colorList = Arrays.asList(new String[]{"white", "black", "red", "blue", "grey", "yellow"});
        List<String> vehicleTypeList = Arrays.asList(new String[]{"Large Truck", "Small Truck", "Private Car", "Bus",
                "Taxi"});

        Random rand = new Random();
        logger.info("Sending events");

        long i = 0L;

        while (true) {

            String ptsId = routeList.get(rand.nextInt(5));
            String vehicleType = vehicleTypeList.get(rand.nextInt(5));
            String plateNumber = generateRandomPlateNumber();
            String color = colorList.get(rand.nextInt(6));
            Date timestamp = new Date();
            double speed = rand.nextInt(120 - 50) + 50;// random speed between 50 to 120

            IoTData event = new IoTData(ptsId, plateNumber, color, speed, vehicleType, timestamp);

//            KeyedMessage<String, IoTData> data = new KeyedMessage<String, IoTData>(topic, event);

            producer.send(new ProducerRecord<String,IoTData>(topic,event.getPlateNumber()+timestamp.toString() , event));
            //Thread.sleep(rand.nextInt(3000 - 1000) + 1000);//random delay of 1 to 3 seconds
            i++;
            Thread.sleep(1000);

        }
    }

    private String generateRandomPlateNumber() {
        String result = "";

        Random rand = new Random();

        List<String> cities = new ArrayList<String>();
        for(int i=1;i<=80;i++)
            cities.add( (String.valueOf(i).length()<2) ? "0"+String.valueOf(i) : String.valueOf(i) );
        String cityCode = cities.get(rand.nextInt(80));

        List<String> strList = Arrays.asList(new String[]{"A","B","C","D","E","F","G","H","K","N"});


        String strCode1 = strList.get(rand.nextInt(10));
        String strCode2 = strList.get(rand.nextInt(10));

        int numCode = rand.nextInt(999-100)+100;


        return cityCode+"_"+strCode1+strCode2+"_"+String.valueOf(numCode);
    }

}
