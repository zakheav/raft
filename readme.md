#raft component
##what is this component?
This component help you to build a strong consistancy system to repicated data. 

for example, you have 5 computer, each of them has a MYSQL database. you want to replicate your data in these 5 MYSQL database, what you should do is run this raft component in this 5 computer, then use raft client to submit your SQL request. 

if over 3(include 3) computers are running well, your system will replicate data to the computers that aren`t dead. When the crash compters reborn, they will update the missing data from others automatically.

Every time you query data through raft client you will get right data if there are over half of computers in the cluster running well.
##how to use this component?
- implements ApplyMethod interface:

  in this class you need to get the connection to the database(such as MYSQL, Redis).   
  override function **apply(String command)**, this function is used to submit your update/delete/insert command

  override function **query(String command)**, this function is used to query the database

    	public class MyApplyMethod implements ApplyMethod {
			@Override
			public void apply(String command) {
				// get connection
				// submit command to database
				System.out.println("applied: "+command);
			}
	
			@Override
			public List<Map<String, Object>> query(String command) {
				// get connection
				// through the conncetion get the query result
				// return the result
			}
		}

- write the conf.xml in conf folder
- start the raft component

