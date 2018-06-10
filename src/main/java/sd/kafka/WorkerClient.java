package sd.kafka;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class WorkerClient {
	public static void main(String[] args) {
		KafkaConsumer<String, String> consumer = Utils.createConsumer();
		consumer.subscribe(Arrays.asList("links"));
		
		// Wait for Jobs to do
		while (true) {
		 // Get a record
		    ConsumerRecords<String, String> records = consumer.poll(1);
		    for (ConsumerRecord<String, String> record : records) {
		   	 Worker w = new Worker(record);
		   	 w.Fetch();
		    }
		}
	}
}
