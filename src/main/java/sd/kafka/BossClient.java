package sd.kafka;

import java.util.Scanner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class BossClient {

	public static void main(String[] args) {
		Producer<String, String> producer = Utils.createProducer();
		Scanner scanIn = new Scanner(System.in);
		
		while(true) {
			System.out.println("Digite uma URL que deseja consultar");
			String url;
			
			url = scanIn.nextLine();
			
			// Finish the program
			if(url.equals("FIM")) {
				break;
			}
			
			String QueryID = Utils.generateId();
			
			// TODO: Create a topic with QueryID and wait for response
			
			// Send the link to the Links topic
		    producer.send(new ProducerRecord<String, String>("links", QueryID, url));
		    
		    // TODO: Creates a topic with QueryID and subscribe to it
		    
		    // TODO: Wait for a worker response
		    System.out.println("Esperando resposta");
		    
		    // TODO: Close the topic
		    
		}
		
		scanIn.close();
		producer.close();
	}

}
