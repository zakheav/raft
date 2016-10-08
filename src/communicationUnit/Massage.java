package communicationUnit;

public class Massage {
	public Massage(ConcurrentSocket socket, String massage) {
		this.socket = socket;
		this.massage = massage;
	}

	public ConcurrentSocket socket;
	public String massage;
}
