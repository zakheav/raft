package serverUnit;

import java.util.ArrayList;
import java.util.List;

public class Server {
	public Server(int nodeNum) {
		this.status = 0;
		this.currentTerm = 1;
		this.grantNum = 1;
		this.commitIndex = 0;
		this.appliedIndex = 0;
		this.log = new ArrayList<LogEntry>();
		this.log.add(new LogEntry(0, null, null));// log下标从1开始
		this.nextIndex = new int[nodeNum];
		this.matchIndex = new int[nodeNum];
		for(int i=0; i<nodeNum; ++i) {
			nextIndex[i] = 1;
			matchIndex[i] = 0;
		}
	}
	
	public int status;
	public int currentTerm;
	public int grantNum;
	public int commitIndex;
	public int appliedIndex;
	public List<LogEntry> log;
	public int[] nextIndex;
	public int[] matchIndex;
	
}
