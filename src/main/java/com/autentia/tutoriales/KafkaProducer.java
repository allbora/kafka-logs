package com.autentia.tutoriales;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.ibr.common.Constantes;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {

	private static final Logger log = Logger.getLogger(KafkaProducer.class);
    private static final String KAFKA_SERVER = "localhost:9092";
	private final Producer<String, String> producer;

    public KafkaProducer() {
        final Properties props = new Properties();
        props.put("metadata.broker.list", KAFKA_SERVER);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        producer = new Producer<String, String>(new ProducerConfig(props));
    }

    public void send(String topic, String message) {
    	log.info("IBR >> sending topic: " + topic + ", message: " + message);
        producer.send(new KeyedMessage<String, String>(topic, message));
    }
    
    public void close() {
    	producer.close();
    }
    
    public static void main(String[] args) {
		new KafkaProducer().send(Constantes.TOPIC, "esto es un test");
	}
}
