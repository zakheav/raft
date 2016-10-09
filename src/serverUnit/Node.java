package serverUnit;

import java.util.ArrayList;
import java.util.List;

import connectionMaintenanceUnit.HelloThread;
import connectionMaintenanceUnit.WelcomeThread;
import raftProcedureUnit.ApplyLogThread;
import raftProcedureUnit.MassageProcessThread;
import timerUnit.TimerThread;

public class Node {
	// 데절
	private static Node instance = new Node();

	private Node() {
		this.nodeAddrListSize = 3;
		this.nodeAddrList = new ArrayList<String>();
		for (int i = 0; i < nodeAddrListSize; ++i) {
			String ip = "127.0.0.1";
			int port = 8080 + i;
			String ipport = ip + ":" + port;
			this.nodeAddrList.add(ipport);
		}
		this.nodeId = 2;
		this.server = new Server(nodeAddrListSize);
	}

	public static Node getInstance() {
		return instance;
	}// 데절

	public int nodeAddrListSize;
	public List<String> nodeAddrList;
	public int nodeId;
	public Server server;

	public void start() {
		new Thread(new HelloThread()).start();
		new Thread(new WelcomeThread()).start();
		new Thread(TimerThread.getInstance()).start();
		new Thread(new MassageProcessThread()).start();
		new Thread(new ApplyLogThread()).start();
	}

	public List<String> get_initiativeConnectAddress() {
		List<String> result = new ArrayList<String>();
		for (int i = 0; i < nodeId; ++i) {
			result.add(nodeAddrList.get(i));
		}
		return result;
	}

	public String get_myAddress() {
		return nodeAddrList.get(nodeId);
	}

	public String get_address(int idx) {
		return nodeAddrList.get(idx);
	}
	
	public static void main(String[] args) {
		Node.getInstance().start();
	}
}
