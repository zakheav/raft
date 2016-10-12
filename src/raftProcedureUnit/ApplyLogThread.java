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

public class ApplyLogThread implements Runnable {

	@Override
	public void run() {
		while (true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (Node.getInstance().server.status == 2) {// 当前节点是leader
				int NodeNum = Node.getInstance().nodeAddrListSize;
				int leaderIdx = Node.getInstance().nodeId;
				int N = Node.getInstance().server.log.get_lastLogIndex();
				boolean findN = false;
				while (!findN) {// 寻找N，使得N>CommitIndex且大部分matchIndex>=N
					int counter = 1;
					for (int i = 0; i < NodeNum; ++i) {
						if (i != leaderIdx) {
							if (Node.getInstance().server.matchIndex[i] >= N) {
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
				Node.getInstance().server.log.commitIndex = Math.max(N, Node.getInstance().server.log.commitIndex);
			}

			// 把AppliedIndex之后，CommitIndex之前（包含CommitIndex）部分的log提交到数据库，同时根据CommandId回复相应的Client
			while (Node.getInstance().server.log.appliedIndex < Node.getInstance().server.log.commitIndex) {
				++Node.getInstance().server.log.appliedIndex;

				// 把这条log存储到持久化存储器上
				int term = Node.getInstance().server.log
						.get_logByIndex(Node.getInstance().server.log.appliedIndex).term;
				String command = Node.getInstance().server.log
						.get_logByIndex(Node.getInstance().server.log.appliedIndex).command;
				String commandId = Node.getInstance().server.log
						.get_logByIndex(Node.getInstance().server.log.appliedIndex).commandId;
				int logIndex = Node.getInstance().server.log
						.get_logByIndex(Node.getInstance().server.log.appliedIndex).index;

				// 如果这条消息没有提交过（防止重复提交）
				if (!Node.getInstance().server.log.checkAppliedBefore(commandId)) {
					String queryString = "insert into log(logIndex, term, command, commandId) values(" + logIndex + ","
							+ term + ",'" + command + "','" + commandId + "')";
					DBpool.getInstance().executeUpdate(queryString);
					// 提交appliedIndex指向的log
					System.out.println("apply: " + command);
				}

				if (Node.getInstance().server.status == 2) {
					// 把提交后结果根据logEntry中的CommandId找到对应的clientSocket返回
					ConcurrentSocket socket = SocketList.getInstance().querySocket(commandId);
					if (socket != null) {
						List<Object> msg9 = new ArrayList<Object>();
						msg9.add(9);
						msg9.add("ok");

						String massage9 = JSON.ArrayToJSON(msg9);
						SendTask task = new SendTask(socket, massage9);
						ThreadPool.getInstance().addTasks(task);
					}
				}
			}
		}
	}
}
