package connectionMaintenanceUnit;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import communicationUnit.ConcurrentSocket;
import communicationUnit.RecvTask;
import communicationUnit.ThreadPool;
import serverUnit.Node;

public class WelcomeThread implements Runnable {

	@Override
	public void run() {
		String addr = Node.getInstance().get_myAddress();
		String[] ip_port = addr.split(":");
		String ip = ip_port[0];
		int port = Integer.parseInt(ip_port[1]);
		
		try {
			@SuppressWarnings("resource")
			ServerSocket serverSocket = new ServerSocket(port, 50, InetAddress.getByName(ip));
			while (true) {
				Socket socket = serverSocket.accept();// 接收主动接入的连接
				ConcurrentSocket cs = new ConcurrentSocket(socket);
				// 打包成RecvTask，加入线程池
				RecvTask task = new RecvTask(cs);
				ThreadPool.getInstance().addTasks(task);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
