package raftProcedureUnit;

import java.util.ArrayList;
import java.util.List;

import communicationUnit.ConcurrentSocket;
import communicationUnit.SendTask;
import communicationUnit.SocketList;
import communicationUnit.ThreadPool;
import serverUnit.Node;
import util.DBpool;
import util.JSON;
import util.XML;

public class ApplyLogThread implements Runnable {
	private ApplyMethod applyMethod;
	
	public ApplyLogThread() {
		String className = new XML().get_applyMethod();
		try {
			Class<?> classObject = Class.forName(className);// 获得指定类的Class对象
			applyMethod = (ApplyMethod)classObject.newInstance();// 调用默认构造函数
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		while (true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (Node.get_instance().server.status == 2) {// 当前节点是leader
				int NodeNum = Node.get_instance().nodeAddrListSize;
				int leaderIdx = Node.get_instance().nodeId;
				int N = Node.get_instance().server.log.get_lastLogIndex();
				boolean findN = false;
				while (!findN) {// 寻找N，使得N>CommitIndex且大部分matchIndex>=N
					int counter = 1;
					for (int i = 0; i < NodeNum; ++i) {
						if (i != leaderIdx) {
							if (Node.get_instance().server.matchIndex[i] >= N) {
								++counter;
							}
						}
					}
					if (counter > NodeNum / 2) {
						findN = true;
					} else {
						--N;
					}
				}
				Node.get_instance().server.log.commitIndex = Math.max(N, Node.get_instance().server.log.commitIndex);
			}

			// 把AppliedIndex之后，CommitIndex之前（包含CommitIndex）部分的log提交到数据库，同时根据CommandId回复相应的Client
			while (Node.get_instance().server.log.appliedIndex < Node.get_instance().server.log.commitIndex) {
				++Node.get_instance().server.log.appliedIndex;

				
				int term = Node.get_instance().server.log
						.get_logByIndex(Node.get_instance().server.log.appliedIndex).term;
				String command = Node.get_instance().server.log
						.get_logByIndex(Node.get_instance().server.log.appliedIndex).command;
				String commandId = Node.get_instance().server.log
						.get_logByIndex(Node.get_instance().server.log.appliedIndex).commandId;
				int logIndex = Node.get_instance().server.log
						.get_logByIndex(Node.get_instance().server.log.appliedIndex).index;

				// 如果这条消息没有提交过（防止重复提交）
				if (!Node.get_instance().server.log.checkAppliedBefore(commandId)) {
					String queryString = "insert into log(logIndex, term, command, commandId) values(" + logIndex + ","
							+ term + ",'" + command + "','" + commandId + "')";
					DBpool.getInstance().executeUpdate(queryString);// 把这条log存储到持久化存储器上
					applyMethod.apply(command);// 提交appliedIndex指向的log
				}

				if (Node.get_instance().server.status == 2) {
					// 把提交后结果根据logEntry中的CommandId找到对应的clientSocket返回
					ConcurrentSocket socket = SocketList.get_instance().querySocket(commandId);
					if (socket != null) {
						List<Object> msg9 = new ArrayList<Object>();
						msg9.add(9);
						msg9.add("ok");

						String massage9 = JSON.ArrayToJSON(msg9);
						SendTask task = new SendTask(socket, massage9);
						ThreadPool.get_instance().add_tasks(task);
					}
				}
			}
		}
	}
}
