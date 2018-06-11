package sd.kafka;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

public class Topic {
	AdminClient admin;
	
	public Topic() {
		Properties props = new Properties();
	    props.put("bootstrap.servers", "localhost:9092");
	     
		this.admin = AdminClient.create(props);
	}
	
	public boolean create(String topicId) throws InterruptedException, ExecutionException {
		ArrayList<NewTopic> topic = new ArrayList<NewTopic>();
		topic.add(new NewTopic(topicId, 1, (short) 1));
		CreateTopicsResult t = this.admin.createTopics(topic);
		
		t.all().get();
		return t.all().isDone();
	}
	
	public boolean delete(String topicId) throws InterruptedException, ExecutionException {
		ArrayList<String> topic = new ArrayList<String>();
		topic.add(topicId);
		
		DeleteTopicsResult r = this.admin.deleteTopics(topic);
		r.all().get();
		
		return r.all().isDone();
	}
	
	public boolean Exists(String topicId) {
		//ListTopicsResult a = this.admin.listTopics();
		return false;
	}
}
