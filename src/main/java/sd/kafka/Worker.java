package sd.kafka;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.kafka.clients.producer.ProducerRecord;

public class Worker {
	private ProducerRecord<String, String> record;
	
	public Worker(ProducerRecord<String, String> record) {
		this.record = record;
	}
	
	/*
	 * Fetch the received Link
	 * Then try to answer it on the topic
	 * if it succeeds then returns True, otherwise returns false
	 */
	public boolean Fetch() {
		String result;
		try {
			result = this.GetHtml(this.record.value());
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
