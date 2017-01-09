package communicationUnit;

import java.io.IOException;
import java.util.List;

public class RecvTask implements Runnable {
	private final ConcurrentSocket socket;

	public RecvTask(ConcurrentSocket socket) {
		this.socket = socket;
	}

	@Override
	public void run() {
		try {
			while (true) {
				List<String> msgList = socket.read();// get the massage
				if(msgList.isEmpty()) {
					System.out.println("client connection has been closed.");
					this.socket.close();// close the "half-open" connection
					break;// finish this task
				} else {
					for(String msg : msgList) {
						if (!msg.isEmpty()) {
							Massage massage = new Massage(this.socket, msg);
							MassageQueue.get_instance().add_massage(massage);// put the massage into the massage queue
						}
					}
				}
			}
		} catch (IOException e) {
			SocketList.get_instance().remove_socket(socket);
		}
	}
}
