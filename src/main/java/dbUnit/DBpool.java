package dbUnit;

import java.util.List;
import java.util.Map;

public interface DBpool {
	public List<Map<String, Object>> executeQuery(String queryString);
	public boolean executeUpdate(List<String> queryStringList);
	public Integer get_lastLogIndex();
	public Integer get_lastLogTerm(Integer lastLogIndex);
	public List<Map<String, Object>> get_recentSubmitCommandId(Integer temp);
	public List<Map<String, Object>> get_logByIndex(Integer index);
	public void build_log();
	public void commit_command(Integer logIndex, Integer term, String command, String commandId);
}
