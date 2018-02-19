package serverUnit;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import dbUnit.DB;

public class Log {
	private LinkedList<LogEntry> log;
	private LinkedList<String> recentAppliedCmd;// the recent command
	private boolean indexCacheDirty = false;
	private boolean termCacheDirty = false;
	private int lastLogIndexCache = -1;
	private int lastLogTermCache = -1;

	public int commitIndex;
	public int appliedIndex;

	public Log() {
		DB.dbpool.build_log();
		
		this.log = new LinkedList<LogEntry>();

		lastLogIndexCache = DB.dbpool.get_lastLogIndex();
		lastLogTermCache = DB.dbpool.get_lastLogTerm(lastLogIndexCache);

		this.commitIndex = get_lastLogIndex();
		this.appliedIndex = this.commitIndex;

		// cache recent submit commandId
		this.recentAppliedCmd = new LinkedList<String>();
		int temp = lastLogIndexCache - 10;
		List<Map<String, Object>> results = DB.dbpool.get_recentSubmitCommandId(temp);
		for (Map<String, Object> row : results) {
			this.recentAppliedCmd.add((String) row.get("commandId"));
		}
	}

	public synchronized int get_lastLogIndex() {
		if (indexCacheDirty) {
			if (log.isEmpty()) {
				lastLogIndexCache = DB.dbpool.get_lastLogIndex();
			} else {
				lastLogIndexCache = log.peekLast().index;
			}
			indexCacheDirty = false;
		}
		return lastLogIndexCache;
	}

	public synchronized int get_lastLogTerm() {
		if (termCacheDirty) {
			if (log.isEmpty()) {
				lastLogTermCache = DB.dbpool.get_lastLogTerm(get_lastLogIndex());
			} else {
				lastLogTermCache = log.peekLast().term;
			}
			termCacheDirty = false;
		}
		return lastLogTermCache;
	}

	public synchronized LogEntry get_logByIndex(int index) {
		if (index == 0) {
			return new LogEntry(0, null, null, 0);
		}
		if (log.isEmpty() || log.peekFirst().index > index) {
			List<Map<String, Object>> entryList = DB.dbpool.get_logByIndex(index);
			if (entryList.isEmpty()) {
				return null;
			} else {
				Map<String, Object> entry = entryList.get(0);
				return new LogEntry((Integer) entry.get("term"), (String) entry.get("command"),
						(String) entry.get("commandId"), (Integer) entry.get("logIndex"));
			}
		} else {
			for (LogEntry logEntry : log) {
				if (logEntry.index == index) {
					return logEntry;
				}
			}
		}
		return null;
	}
	
	// delete log which index >= begin
	public synchronized void delete_logEntry(int begin) {
		Iterator<LogEntry> it = log.iterator();
		while (it.hasNext()) {
			LogEntry log = it.next();
			if (log.index >= begin) {
				it.remove();
			}
		}
		indexCacheDirty = true;
		termCacheDirty = true;
	}

	public synchronized void add_logEntry(int term, String command, String commandId) {
		int logIndex = get_lastLogIndex() + 1;
		LogEntry logEntry = new LogEntry(term, command, commandId, logIndex);
		log.add(logEntry);
		indexCacheDirty = true;
		termCacheDirty = true;
		
		if (this.recentAppliedCmd.size() >= 50) {
			this.recentAppliedCmd.removeFirst();
		}
		this.recentAppliedCmd.add(commandId);
	}
	
	// clean unnecessary log in memory
	public synchronized void logClear() {
		Iterator<LogEntry> it = log.iterator();
		while (it.hasNext()) {
			if (it.next().index < appliedIndex / 2) {
				it.remove();
			}
		}
	}

	public boolean checkAppliedBefore(String commandId) {
		for (String cmdId : this.recentAppliedCmd) {
			if (cmdId.equals(commandId)) {
				return true;
			}
		}
		if (this.recentAppliedCmd.size() >= 50) {
			this.recentAppliedCmd.removeFirst();
		}
		this.recentAppliedCmd.add(commandId);
		return false;
	}
}
