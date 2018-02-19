package communicationUnit;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import util.Integer_byte_transform;

public class ConcurrentSocket {
	public Socket socket;
	private Logger log = Logger.getLogger(ConcurrentSocket.class);
	private byte[] bagCache = new byte[1024 * 10];
	private int payloadLength = -1;
	private int bagWriteIdx = 0;

	public ConcurrentSocket(Socket socket) {
		this.socket = socket;
	}

	private byte[] wrap(byte[] payload) {// add payload length to the bag
		byte[] bag = new byte[payload.length + 4];
		byte[] payloadLength = Integer_byte_transform.intToByteArray(payload.length);
		bag[0] = payloadLength[0];
		bag[1] = payloadLength[1];
		bag[2] = payloadLength[2];
		bag[3] = payloadLength[3];
		for (int i = 0; i < payload.length; ++i) {
			bag[i + 4] = payload[i];
		}
		return bag;
	}
	
	public synchronized void write(String massage) throws IOException {
		byte[] msg = wrap(massage.getBytes());
		try {
			OutputStream output = socket.getOutputStream();
			output.write(msg);
			output.flush();
		} catch (IOException e) {
			log.error("socket error");
			throw e;
		}
	}

	public List<String> read() throws IOException {// solve adhesive bag
		List<String> payloadList = new ArrayList<String>();

		byte[] buffer = new byte[1024 * 10];
		InputStream input = socket.getInputStream();
		int length = input.read(buffer);
		
		for (int bufferIdx = 0; bufferIdx < length; ++bufferIdx) {
			if (bagWriteIdx < 4) {// bag head is not complete, try to complete the bag head
				bagCache[bagWriteIdx++] = buffer[bufferIdx];
			} else {// bag head complete
				if (payloadLength == -1) {
					byte[] lengthByte = new byte[4];
					lengthByte[0] = bagCache[0];
					lengthByte[1] = bagCache[1];
					lengthByte[2] = bagCache[2];
					lengthByte[3] = bagCache[3];
					payloadLength = Integer_byte_transform.byteArrayToInt(lengthByte);
				}
				if (bagWriteIdx < payloadLength + 4) {
					bagCache[bagWriteIdx++] = buffer[bufferIdx];
				}
				if(bagWriteIdx == payloadLength + 4) {
					// this bag receive complete
					StringBuffer massageBuffer = new StringBuffer(1024 * 10);
					for (int i = 4; i < payloadLength + 4; ++i) {
						massageBuffer.append((char) bagCache[i]);
					}
					payloadList.add(massageBuffer.toString());
					// prepare to read next bag
					payloadLength = -1;
					bagWriteIdx = 0;
				}
			}
		}
		
		return payloadList;
	}

	public void close() {
		try {
			this.socket.close();
		} catch (IOException e) {
			System.out.println("socket error");
		}
	}
}
