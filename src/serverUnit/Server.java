package serverUnit;

public class Server {
	public Server(int nodeNum) {
		
		this.status = 0;
		this.grantNum = 1;
		this.log = new Log();
		this.currentTerm = this.log.get_lastLogTerm();
		
		this.nextIndex = new int[nodeNum];
		this.matchIndex = new int[nodeNum];
		for (int i = 0; i < nodeNum; ++i) {
			nextIndex[i] = 1;
			matchIndex[i] = 0;
		}
		
		// 启动log清理线程
		new Thread(new LogReleaseThread()).start();
	}

	public int status;
	public int currentTerm;
	public int grantNum;
	public Log log;
	public int[] nextIndex;
	public int[] matchIndex;

}
