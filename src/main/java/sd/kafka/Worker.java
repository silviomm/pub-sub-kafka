package sd.kafka;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Worker {
	private ConsumerRecord<String, String> record;
	private Producer<String, String> producer;
	public int points;

	public Worker(ConsumerRecord<String, String> record) {
		this.record = record;
		this.points = 0;
		this.producer = Utils.createProducer();
		System.out.println("Iniciando worker");
	}

	public void Fetch() {
		String result;
		try {
			result = this.GetHtml(this.record.value());
			System.out.println("HTML pego");
		} catch (Exception e) {
			System.out.println("Erro ao pegar HTML");
			result = "ERRO AO PEGAR HTML";
		}

		System.out.println("Enviando para: " + this.record.key());
		this.producer.send(new ProducerRecord<String, String>(this.record.key(), result));
		System.out.println("Enviado");

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
