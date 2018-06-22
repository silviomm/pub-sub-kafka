package sd.kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class BossClient {

	public static void main(String[] args) throws Exception {
		Boss b = new Boss();
		Scanner scanner = new Scanner(System.in);

		if (selectOption(scanner)) {
			System.out.println("Running from file...");
			runFromFile(b);
		} else {
			System.out.println("Running from input...");
			runFromInput(b, scanner);
		}

		scanner.close();
	}

	private static void runFromInput(Boss b, Scanner scanner) {
		while (true) {
			System.out.println("Digite uma URL que deseja consultar");
			String url = scanner.nextLine();
			// Finish the program
			if (url.equals("FIM")) {
				break;
			}

			try {
				crawlHtml(b, url);
			} catch (QueueException e) {
				System.out.println(e.getMessage());
			}
		}
	}

	private static void runFromFile(Boss b) {
		String absolutePath = System.getProperty("user.dir") + System.getProperty("file.separator") + "1msites.txt";
		File file = new File(absolutePath);
		List<CompletableFuture<String>> futures = new ArrayList<>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String url;
			while ((url = reader.readLine()) != null) {
				try {
					System.out.println("Mandando URL: " + url);
					futures.add(crawlHtml(b, url));
				} catch (QueueException e) {
					System.out.println(e.getMessage());
				}
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		CompletableFuture<Void> allFuturesWait = CompletableFuture
				.allOf(futures.toArray(new CompletableFuture[futures.size()]));
		try {
			allFuturesWait.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}

	}

	private static CompletableFuture<String> crawlHtml(Boss b, String url) throws QueueException {
		String queueID = b.sendLink(url);
		CompletableFuture<String> future = b.getResponse(Utils.createConsumer(queueID), queueID);
		future.whenComplete((str, error) -> {
			if (error == null) {
				if (str.equals("ERRO AO PEGAR HTML"))
					System.out.println("Recebida resposta de erro de: " + queueID);
				else
					System.out.println("HTML de: " + queueID + "recebido");
				TopicService topicUtils = new TopicService();
				boolean isDeleted = topicUtils.delete(queueID);
				System.out.println("Topic: " + queueID + " deleted: " + isDeleted);
			}
		});
		return future;
	}

	private static boolean selectOption(Scanner scanner) {
		System.out.println("digite 'file' para input de arquivo ou 'msg' para enviar seus links...");
		String option = scanner.nextLine();
		if (option.equals("file"))
			return true;
		else if (option.equals("msg")) {
			return false;
		} else {
			return selectOption(scanner);
		}
	}

}
