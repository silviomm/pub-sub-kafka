package sd.kafka;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class WorkerClient {
	public static void main(String[] args) {
		KafkaConsumer<String, String> consumer = Utils.createConsumer("worker");
		consumer.subscribe(Arrays.asList("links"));
		
		// Wait for Jobs to do
		while (true) {
			// Get a record
		    ConsumerRecords<String, String> records = consumer.poll(60);
		    for (ConsumerRecord<String, String> record : records) {
		    	System.out.println("Link recebido: " + record.value());
		   	 Worker w = new Worker(record);
		   	 w.Fetch();
		    }
		}
	}
}
