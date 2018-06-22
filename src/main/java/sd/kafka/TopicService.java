package sd.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

public class TopicService {

	private AdminClient admin;
	private static final int MAX_WAIT_TIME = 10;

	public TopicService() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "146.164.4.16:9092");

		this.admin = AdminClient.create(props);
	}

	public boolean create(String name) {
		List<NewTopic> topics = new ArrayList<>();
		topics.add(new NewTopic(name, 1, (short) 1));
		CreateTopicsResult c = this.admin.createTopics(topics);

		try {
			c.all().get(MAX_WAIT_TIME, TimeUnit.SECONDS);
			return c.all().isDone();
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean delete(String topicId) {
		List<String> topic = new ArrayList<>();
		topic.add(topicId);
		DeleteTopicsResult d = this.admin.deleteTopics(topic);

		try {
			d.all().get(MAX_WAIT_TIME, TimeUnit.SECONDS);
			return true;
		} catch (TimeoutException | InterruptedException | ExecutionException e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean exists(String topicName) {
		ListTopicsResult list = this.admin.listTopics();
		KafkaFuture<Set<String>> futureNames = list.names();
		
		try {
			Set<String> names = futureNames.get(MAX_WAIT_TIME, TimeUnit.SECONDS);
			return names.contains(topicName) ? true : false;
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			e.printStackTrace();
			return true;
		}
	}
}
