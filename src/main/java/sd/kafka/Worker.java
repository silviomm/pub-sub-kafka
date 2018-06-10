package sd.kafka;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class Worker {
	private ConsumerRecord<String,String> record;
	public int points;
	
	public Worker(ConsumerRecord<String,String> record) {
		this.record = record;
		this.points = 0;
	}
	
	/*
	 * Fetch the received Link
	 * Then try to answer it on the topic
	 * if it succeeds then returns True, otherwise returns false
	 */
	public boolean Fetch() {
		String result;
		try {
			// Get the HTML of the Page
			result = this.GetHtml(this.record.value());
			
			System.out.println(result);
			
			// TODO: Check if the topic with the record.key() exist
			
			// TODO: If true, Publish in the topic, else, return false
			
			// TODO: 
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	private String GetHtml(String urlToRead) throws Exception {
		StringBuilder result = new StringBuilder();
		URL url = new URL(urlToRead);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		String line;
		while ((line = rd.readLine()) != null) {
		   result.append(line);
		}
		rd.close();
		return result.toString();
	}
}
