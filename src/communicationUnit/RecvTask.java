package communicationUnit;

import java.io.IOException;

public class RecvTask implements Runnable {
	private ConcurrentSocket socket;

	public RecvTask(ConcurrentSocket socket) {
		this.socket = socket;
	}

	@Override
	public void run() {
		try {
			while (true) {
				String msg = socket.read();// 得到消息

				if (!msg.isEmpty()) {// 消息非空
					Massage massage = new Massage(this.socket, msg);
					MassageQueue.getInstance().add_massage(massage);// 加入到消息队列
				} else {// 是client端的socket关闭信息
					System.out.println("客户端连接断开");
					this.socket.close();// 删除掉半开连接
					break;// 跳出循环，线程结束这个task
				}
			}
		} catch (IOException e) {
			SocketList.getInstance().remove_socket(socket);
		}
	}
}
