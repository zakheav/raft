package myRaft;

import java.util.List;
import java.util.Map;

import raftProcedureUnit.ApplyMethod;
import util.DBpool;

public class MyApplyMethod implements ApplyMethod {
	@Override
	public void apply(String command) {
		System.out.println("applied: "+command);
	}

	@Override
	public List<Map<String, Object>> query(String command) {
		return DBpool.get_instance().executeQuery(command);
	}
}
