package communicationUnit;

import java.io.IOException;

public class SendTask implements Runnable{
	private final ConcurrentSocket socket;
	private final String massage;
	public SendTask(ConcurrentSocket socket, String massage) {
		this.socket = socket;
		this.massage = massage;
	}
	@Override
	public void run() {
		try {
			socket.write(massage);
		} catch (IOException e) {
			SocketList.get_instance().remove_socket(socket);
		}
	}
}
