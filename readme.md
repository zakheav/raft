# raft component
## what is this component?
This component help you to build a strong consistancy system to replicated data. 

For example, you have 5 computers, each of them has a MYSQL database. You want to replicate your data in these 5 MYSQL databases, what you should do is run this raft component in these 5 computers, then use raft client to submit your SQL request. 

If over 3(include 3) computers are running well, your system will replicate data to the computers that aren`t dead. When the crash compters reborn, they will update the missing data from others automatically.

Every time you query data through raft client you will get right data if there are over half of computers in the cluster running well.
## how to use this component?
- write the conf.xml in conf folder
- start the raft component(**myRaft.Main.main()**)

## what you need to do to make raft apply to other database?
- build a class in dbUnit package and implements interface DBpool
- register your class in the DB class like below:

`public class DB {
	public static DBpool dbpool = new MysqlDBpool();// register your pool object 
}`

