package util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import communicationUnit.ConcurrentSocket;
import communicationUnit.SendTask;
import communicationUnit.SocketList;
import communicationUnit.ThreadPool;

public class QueryTask implements Runnable {

	private String command;
	private String commandId;

	public QueryTask(String command, String commandId) {
		this.command = command;
		this.commandId = commandId;
	}

	@Override
	public void run() {
		List<Map<String, Object>> resultMap = DBpool.getInstance().executeQuery(command);// ²éÑ¯Êý¾Ý
		String result = resultMap.toString();
		ConcurrentSocket socket = SocketList.getInstance().querySocket(commandId);
		if (socket != null) {

			List<Object> msg9 = new ArrayList<Object>();
			msg9.add(9);
			msg9.add(result);

			String massage9 = JSON.ArrayToJSON(msg9);
			SendTask task = new SendTask(socket, massage9);
			ThreadPool.getInstance().addTasks(task);
		}
	}

}
