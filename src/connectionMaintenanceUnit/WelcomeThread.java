package connectionMaintenanceUnit;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.Logger;

import communicationUnit.ConcurrentSocket;
import communicationUnit.RecvTask;
import communicationUnit.ThreadPool;
import serverUnit.Node;

public class WelcomeThread implements Runnable {
	private Logger log = Logger.getLogger(WelcomeThread.class);
	@Override
	public void run() {
		String addr = Node.get_instance().get_myAddress();
		String[] ip_port = addr.split(":");
		String ip = ip_port[0];
		int port = Integer.parseInt(ip_port[1]);
		
		try {
			@SuppressWarnings("resource")
			ServerSocket serverSocket = new ServerSocket(port, 50, InetAddress.getByName(ip));
			while (true) {
				Socket socket = serverSocket.accept();// receive remote socket
				ConcurrentSocket cs = new ConcurrentSocket(socket);
				// build a thread and waiting for massage
				RecvTask task = new RecvTask(cs);
				ThreadPool.get_instance().add_tasks(task);
			}
		} catch (IOException e) {
			log.error(e);
		}
	}

}
