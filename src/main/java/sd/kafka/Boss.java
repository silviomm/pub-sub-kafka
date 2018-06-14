package sd.kafka;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Boss {
	private Producer<String, String> Producer;
	private TopicService _topicService;

	public Boss() {
		this.Producer = Utils.createProducer();
		this._topicService = new TopicService();
	}

	public String sendLink(String url) throws QueueException {
		String queueID = this.genQueueID();

		if (this._topicService.create(queueID)) {
			this.Producer.send(new ProducerRecord<String, String>("links", queueID, url));
		} else {
			throw new QueueException(queueID, url);
		}

		return queueID;
	}

	private String genQueueID() {
		String queueID;
		do {
			queueID = Utils.generateId();
		} while (this._topicService.exists(queueID));
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