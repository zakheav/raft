package communicationUnit;

public class Massage {
	public Massage(ConcurrentSocket socket, String massage) {
		this.socket = socket;
		this.massage = massage;
	}

	public final ConcurrentSocket socket;
	public final String massage;
}
