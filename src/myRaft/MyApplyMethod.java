package myRaft;

import raftProcedureUnit.ApplyMethod;

public class MyApplyMethod implements ApplyMethod {
	@Override
	public void apply(String command) {
		System.out.println("applied: "+command);
	}
}
