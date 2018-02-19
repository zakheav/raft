package connectionMaintenanceUnit;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import communicationUnit.ConcurrentSocket;
import communicationUnit.RecvTask;
import communicationUnit.SendTask;
import communicationUnit.SocketList;
import communicationUnit.ThreadPool;
import serverUnit.Node;
import util.JSON;


public class HelloThread implements Runnable {
	private Logger log = Logger.getLogger(HelloThread.class);
	@Override
	public void run() {
		List<String> nodeList = Node.get_instance().get_initiativeConnectAddress();
		for (String addr : nodeList) {
			String[] ip_port = addr.split(":");
			String ip = ip_port[0];
			int port = Integer.parseInt(ip_port[1]);
			try {
				Socket socket = new Socket(ip, port);// connect remote node successfully
				ConcurrentSocket cs = new ConcurrentSocket(socket);
				SocketList.get_instance().add_helloSocket(cs, addr);// put the socket into the socketList
				
				List<Object> msg5 = new ArrayList<Object>();
				int type = 5;
				String myIpport = Node.get_instance().get_myAddress();
				msg5.add(type);
				msg5.add(myIpport);
				String massage = JSON.ArrayToJSON(msg5);// get own ip:port
				SendTask sendTask = new SendTask(cs, massage);
				ThreadPool.get_instance().add_tasks(sendTask);// send own ip:port to remote node
				
				// build a thread to wait for response
				RecvTask recvTask = new RecvTask(cs);
				ThreadPool.get_instance().add_tasks(recvTask);
				
				log.info("socket connect remote node success, address:" + addr);
			} catch (IOException e) {
				log.info("can`t connect to the remote node, address:" + addr);
				SocketList.get_instance().add_helloSocket(null, addr);
			}
		}
		while (true) {
			try {
				Thread.sleep(1000 * 10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			SocketList.get_instance().reborn_socket();
		}
	}

}
