package sd.kafka;

public class QueueException extends Exception {

	private static final long serialVersionUID = 1L;

	public QueueException(String queueID, String url) {
		super("Error creating queue: " + queueID + "for url: " + url);
	}
	
	public QueueException(String msg) {
		super(msg);
	}
	
}
