package serverUnit;

public class LogEntry {
	public LogEntry(int term, String command, String commandId, int index) {
		this.term = term;
		this.command = command;
		this.commandId = commandId;
		this.index = index;
	}
	
	public final int term;
	public final String command;
	public final String commandId;
	public final int index;
}
