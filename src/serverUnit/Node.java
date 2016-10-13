package serverUnit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import communicationUnit.ThreadPool;
import connectionMaintenanceUnit.HelloThread;
import connectionMaintenanceUnit.WelcomeThread;
import raftProcedureUnit.ApplyLogThread;
import raftProcedureUnit.MassageProcessThread;
import timerUnit.TimerThread;
import util.DBpool;
import util.XML;

public class Node {
	// 单例
	private static Node instance = new Node();

	@SuppressWarnings("unchecked")
	private Node() {
		Map<String, Object> conf = new XML().nodeConf();

		this.nodeAddrListSize = ((List<String>) (conf.get("ipport"))).size();
		this.nodeAddrList = new ArrayList<String>();
		for (String ipport : (List<String>) (conf.get("ipport"))) {
			this.nodeAddrList.add(ipport);
		}
		this.nodeId = Integer.parseInt((String) conf.get("nodeId"));
		this.server = new Server(nodeAddrListSize);
	}

	public static Node getInstance() {
		return instance;
	}// 单例

	public int nodeAddrListSize;
	public List<String> nodeAddrList;
	public int nodeId;
	public Server server;

	public void start() {
		DBpool.getInstance();// 启动连接池
		ThreadPool.getInstance();// 启动线程池
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
