package raftProcedureUnit;

import java.util.ArrayList;
import java.util.List;
import communicationUnit.ConcurrentSocket;
import communicationUnit.Massage;
import communicationUnit.MassageQueue;
import communicationUnit.SendTask;
import communicationUnit.SocketList;
import communicationUnit.ThreadPool;
import serverUnit.Node;
import timerUnit.TimerThread;
import util.JSON;
import util.QueryTask;

public class MassageProcessThread implements Runnable {

	@Override
	public void run() {
		while (true) {
			Massage massage = MassageQueue.get_instance().get_massage();
			System.out.println(massage.massage);
			List<Object> msg = JSON.JSONToArray(massage.massage);
			
			int status = Node.get_instance().server.status;
			int msgType = (int) msg.get(0);
			ConcurrentSocket msgSocket = massage.socket;// get the socket that deliver the massage
			
			if (msgType == 5) {
				String ipport = (String) msg.get(1);
				SocketList.get_instance().move_welcomeSocket(msgSocket, ipport);
			}

			if (status == 0) {// node is follower
				if (msgType == 0) {
					int term = (int) msg.get(1);
					int lastLogIndex = (int) msg.get(2);
					int lastLogTerm = (int) msg.get(3);

					int myTerm = Node.get_instance().server.currentTerm;
					int myLastLogIndex = Node.get_instance().server.log.get_lastLogIndex();
					int myLastLogTerm = Node.get_instance().server.log.get_lastLogTerm();

					List<Object> msg1 = new ArrayList<Object>();// vote response
					if (term > myTerm) {
						if (lastLogTerm > myLastLogTerm
								|| lastLogTerm == myLastLogTerm && lastLogIndex >= myLastLogIndex) {
							Node.get_instance().server.currentTerm = term;// change own term
							TimerThread.get_instance().reset_timer();// reset timer
							// build vote response
							msg1.add(1);
							msg1.add(term);
							msg1.add(true);
						} else {
							// build vote response
							msg1.add(1);
							msg1.add(term);
							msg1.add(false);
						}
					} else {
						// build vote response
						msg1.add(1);
						msg1.add(myTerm);
						msg1.add(false);
					}
					String massage1 = JSON.ArrayToJSON(msg1);
					SendTask task = new SendTask(msgSocket, massage1);
					ThreadPool.get_instance().add_tasks(task);// response vote
				} else if (msgType == 1) {
					// reject
				} else if (msgType == 2) {
					// log copy
					int term = (int) msg.get(1);
					int myTerm = Node.get_instance().server.currentTerm;

					List<Object> msg3 = new ArrayList<Object>();// log copy response
					if (term >= myTerm) {
						Node.get_instance().server.currentTerm = term;// change own term
						TimerThread.get_instance().reset_timer();// reset timer
						// start log copy
						logCopy(msg, msgSocket);
					} else {
						// response: the age has changed
						msg3.add(3);
						msg3.add(myTerm);
						msg3.add(false);
						msg3.add(null);
						msg3.add("agingTerm");
						msg3.add(Node.get_instance().nodeId);
						msg3.add(null);
						String massage3 = JSON.ArrayToJSON(msg3);
						SendTask task = new SendTask(msgSocket, massage3);
						ThreadPool.get_instance().add_tasks(task);// response log copy massage
					}

				} else if (msgType == 3) {
					// reject
				} else if (msgType == 4) {
					TimerThread.get_instance().reset_timer();// reset timer
					++Node.get_instance().server.currentTerm;// increase own term
					Node.get_instance().server.status = 1;// become candidate
					Node.get_instance().server.grantNum = 1;// reset grantNum, prepare to election

					int myTerm = Node.get_instance().server.currentTerm;
					int myLastLogIndex = Node.get_instance().server.log.get_lastLogIndex();
					int myLastLogTerm = Node.get_instance().server.log.get_lastLogTerm();

					List<Object> msg0 = new ArrayList<Object>();// vote massage
					msg0.add(0);
					msg0.add(myTerm);
					msg0.add(myLastLogIndex);
					msg0.add(myLastLogTerm);
					String massage0 = JSON.ArrayToJSON(msg0);
					SocketList.get_instance().broadcast(massage0);// broadcast vote massage
				} else if (msgType == 6) {
					List<Object> msg7 = new ArrayList<Object>();
					msg7.add(7);
					msg7.add(false);
					String massage7 = JSON.ArrayToJSON(msg7);
					SendTask task = new SendTask(msgSocket, massage7);
					ThreadPool.get_instance().add_tasks(task);// response to client: you find a wrong server
				} else if (msgType == 8) {
					List<Object> msg7 = new ArrayList<Object>();
					msg7.add(7);
					msg7.add(false);
					String massage7 = JSON.ArrayToJSON(msg7);
					SendTask task = new SendTask(msgSocket, massage7);
					ThreadPool.get_instance().add_tasks(task);// response to client: you find a wrong server
				}
			} else if (status == 1) {// node is candidate
				if (msgType == 0) {
					int term = (int) msg.get(1);
					int lastLogIndex = (int) msg.get(2);
					int lastLogTerm = (int) msg.get(3);

					int myTerm = Node.get_instance().server.currentTerm;
					int myLastLogIndex = Node.get_instance().server.log.get_lastLogIndex();
					int myLastLogTerm = Node.get_instance().server.log.get_lastLogTerm();

					List<Object> msg1 = new ArrayList<Object>();// vote response
					if (term > myTerm) {
						if (lastLogTerm > myLastLogTerm
								|| lastLogTerm == myLastLogTerm && lastLogIndex >= myLastLogIndex) {
							Node.get_instance().server.status = 0;// node become follower
							Node.get_instance().server.currentTerm = term;// change own term
							TimerThread.get_instance().reset_timer();// reset timer
							// build vote response
							msg1.add(1);
							msg1.add(term);
							msg1.add(true);
						} else {
							// build vote response
							msg1.add(1);
							msg1.add(term);
							msg1.add(false);
						}
					} else {
						// build vote response
						msg1.add(1);
						msg1.add(myTerm);
						msg1.add(false);
					}
					String massage1 = JSON.ArrayToJSON(msg1);
					SendTask task = new SendTask(msgSocket, massage1);
					ThreadPool.get_instance().add_tasks(task);// response vote
				} else if (msgType == 1) {
					int term = (int) msg.get(1);
					boolean grant = (boolean) msg.get(2);
					if (grant) {// agree leader
						++Node.get_instance().server.grantNum;
						if (Node.get_instance().server.grantNum > Node.get_instance().nodeAddrListSize / 2) {
							Node.get_instance().server.status = 2;// become leader
							TimerThread.get_instance().reset_leaderTimer();// reset Leader timer
							for (int i = 0; i < Node.get_instance().nodeAddrListSize; ++i) {// initialize nextIndex[]
								Node.get_instance().server.nextIndex[i] = Node.get_instance().server.log
										.get_lastLogIndex() + 1;
							}
							System.out.println("i am leader");
						}
					} else {
						if (term > Node.get_instance().server.currentTerm) {
							Node.get_instance().server.status = 0;// become follower
							Node.get_instance().server.currentTerm = term;
						}
					}
				} else if (msgType == 2) {
					// log copy
					int term = (int) msg.get(1);
					int myTerm = Node.get_instance().server.currentTerm;

					List<Object> msg3 = new ArrayList<Object>();
					if (term >= myTerm) {
						Node.get_instance().server.status = 0;// become follower
						Node.get_instance().server.currentTerm = term;// change own term
						TimerThread.get_instance().reset_timer();// reset timer
						// start log copy
						logCopy(msg, msgSocket);
					} else {
						// response: the age has changed
						msg3.add(3);
						msg3.add(myTerm);
						msg3.add(false);
						msg3.add(null);
						msg3.add("agingTerm");
						msg3.add(Node.get_instance().nodeId);
						msg3.add(null);
						String massage3 = JSON.ArrayToJSON(msg3);
						SendTask task = new SendTask(msgSocket, massage3);
						ThreadPool.get_instance().add_tasks(task);// response log copy massage
					}

				} else if (msgType == 3) {
					// reject
				} else if (msgType == 4) {
					TimerThread.get_instance().set_timer();// reset timer
					++Node.get_instance().server.currentTerm;// increase term
					Node.get_instance().server.grantNum = 1;// reset grantNum, prepare election

					int myTerm = Node.get_instance().server.currentTerm;
					int myLastLogIndex = Node.get_instance().server.log.get_lastLogIndex();
					int myLastLogTerm = Node.get_instance().server.log.get_lastLogTerm();

					List<Object> msg0 = new ArrayList<Object>();
					msg0.add(0);
					msg0.add(myTerm);
					msg0.add(myLastLogIndex);
					msg0.add(myLastLogTerm);
					String massage0 = JSON.ArrayToJSON(msg0);
					SocketList.get_instance().broadcast(massage0);// broadcast vote massage
				} else if (msgType == 6) {
					List<Object> msg7 = new ArrayList<Object>();
					msg7.add(7);
					msg7.add(false);
					String massage7 = JSON.ArrayToJSON(msg7);
					SendTask task = new SendTask(msgSocket, massage7);
					ThreadPool.get_instance().add_tasks(task);// response to client: find a wrong server
				} else if (msgType == 8) {
					List<Object> msg7 = new ArrayList<Object>();
					msg7.add(7);
					msg7.add(false);
					String massage7 = JSON.ArrayToJSON(msg7);
					SendTask task = new SendTask(msgSocket, massage7);
					ThreadPool.get_instance().add_tasks(task);// response to client: find a wrong server
				}
			} else {// node id leader
				if (msgType == 0) {
					int term = (int) msg.get(1);
					int lastLogIndex = (int) msg.get(2);
					int lastLogTerm = (int) msg.get(3);

					int myTerm = Node.get_instance().server.currentTerm;
					int myLastLogIndex = Node.get_instance().server.log.get_lastLogIndex();
					int myLastLogTerm = Node.get_instance().server.log.get_lastLogTerm();

					List<Object> msg1 = new ArrayList<Object>();// vote response
					if (term > myTerm) {
						if (lastLogTerm > myLastLogTerm
								|| lastLogTerm == myLastLogTerm && lastLogIndex >= myLastLogIndex) {
							Node.get_instance().server.status = 0;// become follower
							SocketList.get_instance().informClientClientClose();
							Node.get_instance().server.currentTerm = term;// change own term
							TimerThread.get_instance().reset_timer();// reset timer
							// build vote response
							msg1.add(1);
							msg1.add(term);
							msg1.add(true);
						} else {
							// build vote response
							msg1.add(1);
							msg1.add(term);
							msg1.add(false);
						}
					} else {
						// build vote response
						msg1.add(1);
						msg1.add(myTerm);
						msg1.add(false);
					}
					String massage1 = JSON.ArrayToJSON(msg1);
					SendTask task = new SendTask(msgSocket, massage1);
					ThreadPool.get_instance().add_tasks(task);// response vote
				} else if (msgType == 1) {
					// reject
				} else if (msgType == 2) {
					// log copy
					int term = (int) msg.get(1);
					int myTerm = Node.get_instance().server.currentTerm;

					List<Object> msg3 = new ArrayList<Object>();
					if (term >= myTerm) {
						Node.get_instance().server.status = 0;// become follower
						SocketList.get_instance().informClientClientClose();
						Node.get_instance().server.currentTerm = term;// change own term
						TimerThread.get_instance().reset_timer();// reset timer
						// start log copy
						logCopy(msg, msgSocket);
					} else {
						// response: age has changed
						msg3.add(3);
						msg3.add(myTerm);
						msg3.add(false);
						msg3.add(null);
						msg3.add("agingTerm");
						msg3.add(Node.get_instance().nodeId);
						msg3.add(null);
						String massage3 = JSON.ArrayToJSON(msg3);
						SendTask task = new SendTask(msgSocket, massage3);
						ThreadPool.get_instance().add_tasks(task);// response log copy
					}

				} else if (msgType == 3) {
					// process log copy response
					int term = (int) msg.get(1);
					boolean success = (boolean) msg.get(2);
					int followerIdx = (int) msg.get(5);
					if (success) {
						int matchIndex = (int) msg.get(3);
						Node.get_instance().server.nextIndex[followerIdx] = matchIndex + 1;
						Node.get_instance().server.matchIndex[followerIdx] = matchIndex;
					} else {
						String failReason = (String) msg.get(4);
						if (failReason.equals("agingTerm")) {// age already change
							Node.get_instance().server.status = 0;// become follower
							SocketList.get_instance().informClientClientClose();
							Node.get_instance().server.currentTerm = term;// change own term
						} else {
							int suggestNextIndex = (int) msg.get(6);
							Node.get_instance().server.nextIndex[followerIdx] = suggestNextIndex;
						}
					}
				} else if (msgType == 4) {
					TimerThread.get_instance().reset_leaderTimer();// reset leader timer
					// send log copy RPC
					int myNodeIdx = Node.get_instance().nodeId;
					for (int i = 0; i < Node.get_instance().nodeAddrListSize; ++i) {
						if (i != myNodeIdx) {
							List<Object> msg2 = new ArrayList<Object>();
							int nextIndex = Node.get_instance().server.nextIndex[i];
							int myLastLogIndex = Node.get_instance().server.log.get_lastLogIndex();

							int myTerm = Node.get_instance().server.currentTerm;
							int prevLogIndex = nextIndex - 1;
							int prevLogTerm = Node.get_instance().server.log.get_logByIndex(prevLogIndex).term;
							int leaderCommit = Node.get_instance().server.log.commitIndex;

							int endIndex = myLastLogIndex - nextIndex > 50 ? nextIndex + 50 : myLastLogIndex;
							int entriesNum = endIndex - nextIndex + 1 > 0 ? endIndex - nextIndex + 1 : 0;
							msg2.add(2);
							msg2.add(myTerm);
							msg2.add(prevLogIndex);
							msg2.add(prevLogTerm);
							msg2.add(leaderCommit);
							msg2.add(entriesNum);
							for (int j = nextIndex; j <= endIndex; ++j) {
								msg2.add(Node.get_instance().server.log.get_logByIndex(j).command);
							}
							for (int j = nextIndex; j <= endIndex; ++j) {
								msg2.add(Node.get_instance().server.log.get_logByIndex(j).commandId);
							}
							for (int j = nextIndex; j <= endIndex; ++j) {
								msg2.add(Node.get_instance().server.log.get_logByIndex(j).term);
							}

							String massage2 = JSON.ArrayToJSON(msg2);
							String remoteAddr = Node.get_instance().get_address(i);
							ConcurrentSocket cs = SocketList.get_instance().querySocket(remoteAddr);
							if (cs != null) {
								SendTask task = new SendTask(cs, massage2);
								ThreadPool.get_instance().add_tasks(task);// send log copy RPC
							}
						}
					}
				} else if (msgType == 6) {
					List<Object> msg7 = new ArrayList<Object>();
					msg7.add(7);
					msg7.add(true);
					String massage7 = JSON.ArrayToJSON(msg7);
					SendTask task = new SendTask(msgSocket, massage7);
					ThreadPool.get_instance().add_tasks(task);// response to client: find right server
				} else if (msgType == 8) {
					int myTerm = Node.get_instance().server.currentTerm;
					boolean read = (boolean) msg.get(1);
					String command = (String) msg.get(2);
					String commandId = (String) msg.get(3);
					SocketList.get_instance().move_clientSocket(msgSocket, commandId);
					if (!read) {// not read request
						Node.get_instance().server.log.add_logEntry(myTerm, command, commandId);// append new logEntry into log
					} else {
						QueryTask task = new QueryTask(command, commandId);
						ThreadPool.get_instance().add_tasks(task);// query
					}
				}
			}
		}
	}

	private void logCopy(List<Object> msg, ConcurrentSocket msgSocket) {
		int term = (int) msg.get(1);
		int prevLogIndex = (int) msg.get(2);
		int prevLogTerm = (int) msg.get(3);
		int leaderCommit = (int) msg.get(4);
		int entriesNum = (int) msg.get(5);

		if (Node.get_instance().server.log.get_logByIndex(prevLogIndex) != null
				&& Node.get_instance().server.log.get_logByIndex(prevLogIndex).term == prevLogTerm) {

			Node.get_instance().server.log.delete_logEntry(prevLogIndex + 1);// delete logEntry in log after prevLogIndex

			for (int entriesIdx = 6; entriesIdx <= 5 + entriesNum; ++entriesIdx) {
				String command = (String) msg.get(entriesIdx);
				String commandId = (String) msg.get(entriesIdx + entriesNum);
				int logTerm = (int) msg.get(entriesIdx + 2 * entriesNum);

				Node.get_instance().server.log.add_logEntry(logTerm, command, commandId);
			} // append new entries into log

			// send success massage
			List<Object> msg3 = new ArrayList<Object>();
			msg3.add(3);
			msg3.add(term);
			msg3.add(true);
			msg3.add(Node.get_instance().server.log.get_lastLogIndex());
			msg3.add(null);
			msg3.add(Node.get_instance().nodeId);
			msg3.add(null);
			String massage3 = JSON.ArrayToJSON(msg3);
			SendTask task = new SendTask(msgSocket, massage3);
			ThreadPool.get_instance().add_tasks(task);

			if (leaderCommit > Node.get_instance().server.log.commitIndex) {
				Node.get_instance().server.log.commitIndex = Math.min(leaderCommit,
						Node.get_instance().server.log.get_lastLogIndex());
			}
		} else {
			// response: log not match 
			List<Object> msg3 = new ArrayList<Object>();
			msg3.add(3);
			msg3.add(term);
			msg3.add(false);
			msg3.add(null);
			msg3.add("noMatching");
			msg3.add(Node.get_instance().nodeId);

			// suggest leader send this nextIndex next time
			if (Node.get_instance().server.log.get_logByIndex(prevLogIndex) == null) {
				msg3.add(Node.get_instance().server.log.get_lastLogIndex() + 1);
			} else {
				msg3.add(prevLogIndex);
			}

			String massage3 = JSON.ArrayToJSON(msg3);
			SendTask task = new SendTask(msgSocket, massage3);
			ThreadPool.get_instance().add_tasks(task);
		}
	}

}
