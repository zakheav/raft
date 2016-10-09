package serverUnit;

public class LogEntry {
	public LogEntry(int term, String command, String commandId, int index) {
		this.term = term;
		this.command = command;
		this.commandId = commandId;
		this.index = index;
	}
	
	public int term;
	public String command;
	public String commandId;
	public int index;
}
