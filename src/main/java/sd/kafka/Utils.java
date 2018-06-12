package sd.kafka;

import java.util.Properties;
import org.apache.commons.lang3.RandomStringUtils;
import java.util.UUID;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

public class Utils {
	public static KafkaConsumer<String, String> createConsumer(String groupId) {
		// Load Properties
	     Properties props = new Properties();
	     props.put("bootstrap.servers", "146.164.4.16:9092");
	     props.put("group.id", groupId);
	     props.put("enable.auto.commit", "true");
	     props.put("auto.commit.interval.ms", "1000");
	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     
	     
	     // Initiate consumer and subscribe to the links topic
	     KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
	     return consumer;
	}
	
	public static Producer<String, String> createProducer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "146.164.4.16:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	
		Producer<String, String> producer = new KafkaProducer<>(props);
		return producer;
	}
	
    public static String generateId() {
        String uuid = UUID.randomUUID().toString();
        return RandomStringUtils.random(8, "0123456789abcdef");
    }
}
