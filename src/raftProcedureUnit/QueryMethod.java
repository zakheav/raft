package raftProcedureUnit;

import java.util.List;
import java.util.Map;

import dbUnit.DB;

public class QueryMethod {
	public List<Map<String, Object>> query(String command) {
		return DB.dbpool.executeQuery(command);
	}
}
