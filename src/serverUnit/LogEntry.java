package serverUnit;

public class LogEntry {
	public LogEntry(int term, String command, String commandId) {
		this.term = term;
		this.command = command;
		this.commandId = commandId;
	}
	public int term;
	public String command;
	public String commandId;
}
