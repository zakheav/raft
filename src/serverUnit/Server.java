package serverUnit;

import java.util.Iterator;
import java.util.LinkedList;

public class Server {
	public Server(int nodeNum) {
		this.status = 0;
		this.currentTerm = 1;
		this.grantNum = 1;
		this.commitIndex = 0;
		this.appliedIndex = 0;
		this.log = new LinkedList<LogEntry>();
		// 从数据库的log表中搜索最后一条log，加入到log中
		// 如果log表是空的，加入如下的信息
		this.log.add(new LogEntry(0, null, null, 0));// log下标从1开始

		this.nextIndex = new int[nodeNum];
		this.matchIndex = new int[nodeNum];
		for (int i = 0; i < nodeNum; ++i) {
			nextIndex[i] = 1;
			matchIndex[i] = 0;
		}
	}

	public int status;
	public int currentTerm;
	public int grantNum;
	public int commitIndex;
	public int appliedIndex;
	public LinkedList<LogEntry> log;
	public int[] nextIndex;
	public int[] matchIndex;

	public int get_lastLogIndex() {
		return log.peekLast().index;
	}
	
	public int get_lastLogTerm() {
		return log.peekLast().term;
	}

	public LogEntry get_logByIndex(int index) {
		if(log.peekFirst().index > index) {
			// 在数据库log表中寻找相关信息
		} else {
			for(LogEntry logEntry : log) {
				if(logEntry.index == index) {
					return logEntry;
				}
			}
		}
		return null;
	}
	
	public void deleteLogEntry(int begin) {// 删除index域大于等于begin的log项
		Iterator<LogEntry> it = log.iterator();
		while(it.hasNext()) {
			LogEntry log = it.next();
			if(log.index >= begin) {
				it.remove();
			}
		}
	}
}
