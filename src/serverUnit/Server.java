package serverUnit;

public class Server {
	public Server(int nodeNum) {
		
		this.status = 0;
		this.currentTerm = 1;
		this.grantNum = 1;
		this.log = new Log();

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
	public Log log;
	public int[] nextIndex;
	public int[] matchIndex;

}
