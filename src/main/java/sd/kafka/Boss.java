package sd.kafka;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Boss {
	Producer<String, String> Producer;
	Topic topicUtils;

	public Boss() {
		this.Producer = Utils.createProducer();
		this.topicUtils = new Topic();

	}

	public String SendLink(String url) throws InterruptedException, ExecutionException {
		String result = "";
		String QueryID = Utils.generateId();

		try {
			this.topicUtils.Create(QueryID);
			this.Producer.send(new ProducerRecord<String, String>("links", QueryID, url));
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}

		KafkaConsumer<String, String> consumer = Utils.createConsumer("boss");
		System.out.println("Waiting response on topic: " + QueryID);
		result = waitResponse(consumer, QueryID);
		topicUtils.Delete(QueryID);

		return result;
	}

	private String waitResponse(KafkaConsumer<String, String> consumer, String QueueId) {
		String result = "";
		consumer.subscribe(Arrays.asList(QueueId));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(1);
			if (records.count() > 0) {
				for (ConsumerRecord<String, String> record : records) {
					result = record.value();
				}
				break;
			}
		}
		return result;
	}
}