package com.iot.kafka;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.producer.KeyedMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
import java.time.LocalDateTime;
import kafka.javaapi.producer.Producer;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import com.iot.sensors.*;
import java.util.Random;
import org.apache.kafka.common.serialization.BytesSerializer;
public class VoltRun {
    public static void main(String args[]){
//        CustomProducer cp = new CustomProducer("192.168.2.13:9092","test1",1);
//        cp.jsonInit();
        Properties producerProps = new Properties();
        producerProps.put("metadata.broker.list", "10.0.0.10:9092");
        producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
        producerProps.put("request.required.acks", "1");
        kafka.producer.ProducerConfig producerConfig = new kafka.producer.ProducerConfig(producerProps);
        Producer producer = new Producer<String, String>(producerConfig);
        Producer producer1 = new Producer<String, String>(producerConfig);
        Producer producer2 = new Producer<String, String>(producerConfig);
        ObjectMapper mapper = new ObjectMapper();
        String topicName = "rhsensor_run2";
        String topicName1 = "psensor_run2";
        String topicName2 = "tsensor_run2";
        RHSensor rhSensor = new RHSensor();
        Random r = new Random();
        StringBuffer rhs = new StringBuffer();
        StringBuffer ps = new StringBuffer();
        StringBuffer ts = new StringBuffer();
        Double latitude;
        Double longitude;

        System.out.println("Starting the Producer messages");

        for (int t = 0; t < 86400; t++) {
            for (int i = 0; i < 100; i++) {
                longitude = Math.random() * Math.PI * 2;
                latitude = Math.acos(Math.random() * 2  -1);
                rhs.append(r.nextInt(6));  // id
                rhs.append(",");
                rhs.append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS"))); //end
                rhs.append(",");
                rhs.append("N"); // rain
                rhs.append(",");
                rhs.append(r.nextInt(99) / 5); //air temprature
                rhs.append(",");
                rhs.append("N"); //quality
                rhs.append(",");
                rhs.append(r.nextDouble() * 20); //h wind speed
                rhs.append(",");
                rhs.append(r.nextDouble() * 45); // relative humidity
                rhs.append(",");
                rhs.append(r.nextInt(90)); // wind direction
                rhs.append(",");
                rhs.append(latitude);
                rhs.append(",");
                rhs.append(longitude);

                // Pressure Sensor
                ps.append(r.nextInt(6));  // id
                ps.append(",");
                ps.append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS"))); // Clock time
                ps.append(",");
                ps.append(r.nextLong()); // Altitude
                ps.append(",");
                ps.append(r.nextLong()); // Pressure
                ps.append(",");
                ps.append(r.nextDouble()); // Temperature
                ps.append(",");
                ps.append(latitude); // Latitude
                ps.append(",");
                ps.append(longitude); // Longitude

                // Temperature Sensor
                ts.append(r.nextInt(6));  // id
                ts.append(",");
                ts.append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS"))); // Clock time
                ts.append(",");
                ts.append(r.nextLong()); // Air Temp
                ts.append(",");
                ts.append(r.nextLong()); // Wind Speed
                ts.append(",");
                ts.append(r.nextDouble()); // Surface Temperature
                ts.append(",");
                ts.append(latitude); // Latitudebcdmps
                ts.append(",");
                ts.append(longitude); // Longitude
//            sb.append(",");
//            sb.append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS"))); //end
//            sb.append("null");
                //         rhSensor.parseString(sb.toString());
//            System.out.println(sb.toString());
//            JsonNode jsonNode = mapper.valueToTree(rhSensor);
//            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName,sb.toString());
//            producer.send(rec);
                KeyedMessage<String, String> keyedMsg = new KeyedMessage<String, String>(topicName, rhs.toString());
                producer.send(keyedMsg);
                rhs.setLength(0); // resetting the buffer


                KeyedMessage<String, String> keyedMsg1 = new KeyedMessage<String, String>(topicName1, ps.toString());
                producer.send(keyedMsg1);
                ps.setLength(0);

                KeyedMessage<String, String> keyedMsg2 = new KeyedMessage<String, String>(topicName2, ts.toString());
                producer.send(keyedMsg2);
                ts.setLength(0);
            }
                try {
                    Thread.sleep(1000); // Wait for 1 min
                }catch (InterruptedException e){
                    e.printStackTrace();
                }

        } // Ending time loop
        producer.close();
    }
}
