package util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import communicationUnit.ConcurrentSocket;
import communicationUnit.SendTask;
import communicationUnit.SocketList;
import communicationUnit.ThreadPool;
import raftProcedureUnit.ApplyMethod;

public class QueryTask implements Runnable {

	private final String command;
	private final String commandId;
	private ApplyMethod applyMethod;

	public QueryTask(String command, String commandId) {
		this.command = command;
		this.commandId = commandId;
		String className = new XML().get_applyMethod();
		try {
			Class<?> classObject = Class.forName(className);
			this.applyMethod = (ApplyMethod) classObject.newInstance();
		} catch (Exception e) {
			this.applyMethod = null;
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		List<Map<String, Object>> resultMap = applyMethod.query(command);
		String result = JSON.ListToJSON(resultMap);
		ConcurrentSocket socket = SocketList.get_instance().querySocket(commandId);
		if (socket != null) {

			List<Object> msg9 = new ArrayList<Object>();
			msg9.add(9);
			msg9.add(result);

			String massage9 = JSON.ArrayToJSON(msg9);
			SendTask task = new SendTask(socket, massage9);
			ThreadPool.get_instance().add_tasks(task);
		}
	}

}
