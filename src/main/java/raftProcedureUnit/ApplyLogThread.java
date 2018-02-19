package raftProcedureUnit;

import java.util.ArrayList;
import java.util.List;

import communicationUnit.ConcurrentSocket;
import communicationUnit.SendTask;
import communicationUnit.SocketList;
import communicationUnit.ThreadPool;
import dbUnit.DB;
import serverUnit.Node;
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
			if (Node.get_instance().server.status == 2) {// node is leader
				int NodeNum = Node.get_instance().nodeAddrListSize;
				int leaderIdx = Node.get_instance().nodeId;
				int N = Node.get_instance().server.log.get_lastLogIndex();
				boolean findN = false;
				while (!findN) {// find a N the N>CommitIndex and most of matchIndex>=N
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

			// submit log that after AppliedIndex, before CommitIndex (include CommitIndex) into database, response to Client
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

				// check no duplicate
				if (!Node.get_instance().server.log.checkAppliedBefore(commandId)) {
					DB.dbpool.commit_command(logIndex, term, command, commandId);
				}

				if (Node.get_instance().server.status == 2) {
					// response to Client
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
