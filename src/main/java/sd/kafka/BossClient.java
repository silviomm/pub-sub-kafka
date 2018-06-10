package sd.kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class BossClient {

	public static void main(String[] args) {
		Producer<String, String> producer = Utils.createProducer();

		if (selectOption()) {
			System.out.println("Running from file...");
			runFromFile(producer);
		} else {
			System.out.println("Running from input...");
			runFromInput(producer);
		}

		producer.close();
	}

	private static void runFromInput(Producer<String, String> producer) {
		Scanner scanIn = new Scanner(System.in);

		while (true) {
			System.out.println("Digite uma URL que deseja consultar");
			String url;

			url = scanIn.nextLine();

			// Finish the program
			if (url.equals("FIM")) {
				break;
			}

			String QueryID = Utils.generateId();
			// TODO: Create a topic to wait for response

			// Send the link to the Links topic
			producer.send(new ProducerRecord<String, String>("links", QueryID, url));

			// TODO: Creates a topic with QueryID and subscribe to it

			// TODO: Wait for a worker response
			System.out.println("Esperando resposta");

			// TODO: Close the topic
		}
	}

	private static void runFromFile(Producer<String, String> producer) {

		String absolutePath = System.getProperty("user.dir") + System.getProperty("file.separator") + "sites.txt";
		File file = new File(absolutePath);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String url;
			while((url = reader.readLine()) != null) {
				String QueryID = Utils.generateId();
				producer.send(new ProducerRecord<String, String>("links", QueryID, url));
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	private static boolean selectOption() {
		Scanner scanner = new Scanner(System.in);
		System.out.println("digite 'file' para input de arquivo ou 'msg' para enviar seus links...");
		String option = scanner.nextLine();
		scanner.close();
		if (option.equals("file"))
			return true;
		return false;
	}

}
