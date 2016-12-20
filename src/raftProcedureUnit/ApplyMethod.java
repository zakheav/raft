package raftProcedureUnit;

import java.util.List;
import java.util.Map;

public interface ApplyMethod {
	public void apply(String command);
	public List<Map<String, Object>> query(String command);
}
