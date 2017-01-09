package communicationUnit;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

public class ConcurrentSocket {
	public Socket socket;
	private Logger log = Logger.getLogger(ConcurrentSocket.class);

	public ConcurrentSocket(Socket socket) {
		this.socket = socket;
	}

	public synchronized void write(String massage) throws IOException {
		byte[] msg = massage.getBytes();
		try {
			OutputStream output = socket.getOutputStream();
			output.write(msg);
			output.flush();
		} catch (IOException e) {
			log.error("socket error");
			throw e;
		}
	}

	public String read() throws IOException {// adhesive bag need to solve
		byte[] buffer = new byte[1024 * 10];
		InputStream input = socket.getInputStream();
		int length = input.read(buffer);

		StringBuffer massageBuffer = new StringBuffer(1024 * 10);
		for (int i = 0; i < length; ++i) {
			massageBuffer.append((char) buffer[i]);
		}
		return massageBuffer.toString();
	}
	
	public void close() {
		try {
			this.socket.close();
		} catch (IOException e) {
			System.out.println("socket error");
		}
	}
}
