package sd.kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class BossClient {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
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

	private static void runFromInput(Boss b, Scanner scanner) throws InterruptedException, ExecutionException {
		while (true) {
			System.out.println("Digite uma URL que deseja consultar");
			String url;

			url = scanner.nextLine();

			// Finish the program
			if (url.equals("FIM")) {
				break;
			}

			String response = b.SendLink(url);
			System.out.println(response);
		}
	}

	private static void runFromFile(Boss b) {
		String absolutePath = System.getProperty("user.dir") + System.getProperty("file.separator") + "sites.txt";
		File file = new File(absolutePath);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String url;
			while((url = reader.readLine()) != null) {
				String response = b.SendLink(url);
				System.out.println(response);
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	private static boolean selectOption(Scanner scanner) {
		System.out.println("digite 'file' para input de arquivo ou 'msg' para enviar seus links...");
		String option = scanner.nextLine();
		if (option.equals("file"))
			return true;
		return false;
	}

}
