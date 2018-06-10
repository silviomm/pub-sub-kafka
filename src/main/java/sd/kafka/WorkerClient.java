package sd.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class WorkerClient {
	public static void main(String[] args) {
		// Load Properties
	     Properties props = new Properties();
	     props.put("bootstrap.servers", "localhost:9092");
	     props.put("group.id", "consumer");
	     props.put("enable.auto.commit", "true");
	     props.put("auto.commit.interval.ms", "1000");
	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     
	     
	     // Initiate consumer and subscribe to the links topic
	     KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
	     consumer.subscribe(Arrays.asList("links"));
	     
	     // Wait for Jobs to do
	     while (true) {
	         ConsumerRecords<String, String> records = consumer.poll(100);
	         for (ConsumerRecord<String, String> record : records) {
	             System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
	         }
	     }
	}
}
