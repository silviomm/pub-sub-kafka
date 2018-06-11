package sd.kafka;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

	public String sendLink(String url) throws InterruptedException, ExecutionException {
		String queueID = Utils.generateId();

		try {
			this.topicUtils.create(queueID);
			this.Producer.send(new ProducerRecord<String, String>("links", queueID, url));
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}

		return queueID;
	}

	public Future<String> getResponse(KafkaConsumer<String, String> consumer, String QueueId) {
		Callable<String> task = new Callable<String>() {
			public String call() {
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
		};
		ExecutorService executor =  Executors.newSingleThreadExecutor();
		return executor.submit(task);
	}
}