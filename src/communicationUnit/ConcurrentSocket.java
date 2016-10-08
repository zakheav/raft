package communicationUnit;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class ConcurrentSocket {
	public Socket socket;

	public ConcurrentSocket(Socket socket) {
		this.socket = socket;
	}

	public synchronized void write(String massage) throws IOException {
		byte[] msg = massage.getBytes();
		try {
			OutputStream output = socket.getOutputStream();
			output.write(msg);// 阻塞
			output.flush();
		} catch (IOException e) {
			System.out.println("socket error");
			throw e;
		}
	}

	public String read() throws IOException {
		byte[] buffer = new byte[1024*10];
		InputStream input = socket.getInputStream();
		int length =  input.read(buffer);// 阻塞
		
		StringBuffer massageBuffer = new StringBuffer(1024 * 10);
		for (int i = 0; i < length; ++i) {
			massageBuffer.append((char) buffer[i]);
		}
		return massageBuffer.toString();// 得到消息
	}

	public void close() {
		try {
			this.socket.close();
		} catch (IOException e) {
			System.out.println("socket已经关闭");
		}
	}
}
