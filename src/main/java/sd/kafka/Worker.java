package sd.kafka;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Worker {
	private ConsumerRecord<String,String> record;
	private Producer<String, String> producer;
	public int points;
	
	public Worker(ConsumerRecord<String,String> record) {
		this.record   = record;
		this.points   = 0;
		this.producer = Utils.createProducer();
		System.out.println("Iniciando worker");
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
			
			System.out.println("HTML PEGO");
			System.out.println("Enviando para: " + this.record.key());
			Future<RecordMetadata> a = this.producer.send(new ProducerRecord<String, String>(this.record.key(), result));
			a.get(10, TimeUnit.SECONDS);
			
			System.out.println("Enviado");
			
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
