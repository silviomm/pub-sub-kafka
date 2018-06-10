package sd.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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
	
	public boolean CreateNew(String topicId) throws InterruptedException {
		ArrayList<NewTopic> topic = new ArrayList<NewTopic>();
		topic.add(new NewTopic(topicId, 1, (short) 10));
		CreateTopicsResult t = this.admin.createTopics(topic);
		
		t.wait();
		return t.all().isDone();
	}
	
	public boolean Delete(String topicId) throws InterruptedException {
		ArrayList<String> topic = new ArrayList<String>();
		topic.add(topicId);
		
		DeleteTopicsResult r = this.admin.deleteTopics(topic);
		r.wait();
		
		return r.all().isDone();
	}
}
