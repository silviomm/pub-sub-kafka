package sd.kafka;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Boss {
	private Producer<String, String> Producer;
	private Topic topicUtils;

	public Boss() {
		this.Producer = Utils.createProducer();
		this.topicUtils = new Topic();

	}

	public String sendLink(String url) throws Exception {
		String queueID = this.genQueueID();
		do {
			queueID = Utils.generateId();
		} while (this.topicUtils.exists(queueID));
		
		try {
			if (this.topicUtils.create(queueID)) {
				this.Producer.send(new ProducerRecord<String, String>("links", queueID, url));
			} else {
				throw new Exception("Error creating queue: " + queueID + "for url: " + url);
			}
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}

		return queueID;
	}
	
	private String genQueueID() {
		String queueID;
		do {
			queueID = Utils.generateId();
		} while (this.topicUtils.exists(queueID));
		return queueID;
	}

	public CompletableFuture<String> getResponse(KafkaConsumer<String, String> consumer, String queueId) {
		CompletableFuture<String> task = CompletableFuture.supplyAsync(() -> consumeQueue(consumer, queueId));
		return task;
	}

	private String consumeQueue(KafkaConsumer<String, String> consumer, String queueId) {
		String result = "";
		consumer.subscribe(Arrays.asList(queueId));
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