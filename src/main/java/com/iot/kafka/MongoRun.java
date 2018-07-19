package com.iot.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iot.sensors.PressureSensor;
import com.iot.sensors.RHSensor;
import com.iot.sensors.TempSensor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;



public class MongoRun {
    public static void main(String args[]) {
//        CustomProducer cp = new CustomProducer("192.168.2.13:9092","test1",1);
//        cp.jsonInit();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.0.20:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonSerializer");
        Producer producer = new KafkaProducer(props);
        ObjectMapper mapper = new ObjectMapper();
//        String topicName = "mongotest";

        RHSensor rhSensor = new RHSensor();  //Relative Humidity
        PressureSensor pSensor = new PressureSensor(); // Pressure
        TempSensor tempSensor = new TempSensor(); // Temperature

        Random r = new Random();
        StringBuffer rhs = new StringBuffer();
        StringBuffer ps = new StringBuffer();
        StringBuffer ts = new StringBuffer();
        Double latitude;
        Double longitude;

        System.out.println("Starting the Producer messages");
        for (int t = 1; t < 3600; t++) {  // Change the value of t to adjust the time interval for messages
            for (int i = 0; i < 1000; i++) { // Change the value of i to adjust the number of messages per interval
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


//            System.out.println(rhSensor.toString());
//            JsonNode jsonNode = mapper.valueToTree(rhSensor);

// RH Sensor
            rhSensor.parseString(rhs.toString());
            JsonNode rNode = mapper.valueToTree(rhSensor);
            ProducerRecord<String, JsonNode> rh = new ProducerRecord<String, JsonNode>("run4_mrhsensor", rNode); // Change topic name for each experiment
            producer.send(rh);
            rhs.setLength(0);
//Temperature Sensor
            tempSensor.parseString(ts.toString());
            JsonNode tNode = mapper.valueToTree(tempSensor);
            ProducerRecord<String, JsonNode> temp = new ProducerRecord<String, JsonNode>("run4_mtsensor", tNode); // Change topic name for each experiment
            producer.send(temp);
            ts.setLength(0);

// Pressure Sensor
            pSensor.parseString(ps.toString());
            JsonNode pNode = mapper.valueToTree(pSensor);
            ProducerRecord<String, JsonNode> pressure = new ProducerRecord<String, JsonNode>("run4_mpsensor", pNode); // Change topic name for each experiment
            producer.send(pressure);
            ps.setLength(0);
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } // Control waits for 1 sec to continue the task

    }
    producer.close();
    }
}
