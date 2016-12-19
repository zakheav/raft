package communicationUnit;

import java.util.LinkedList;
import java.util.Queue;

public class MassageQueue {
	// 데절
	private static MassageQueue instance = new MassageQueue();
	private MassageQueue() {
		this.queue = new LinkedList<Massage>();
	}
	public static MassageQueue get_instance() {
		return instance;
	}// 데절
	
	private Queue<Massage> queue;
	
	public Massage get_massage() {
		synchronized (queue) {
			while(queue.isEmpty()) {
				try {
					queue.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			return queue.poll();
		}
	}
	
	public void add_massage(Massage msg) {
		synchronized (queue) {
			queue.add(msg);
			queue.notifyAll();
		}
	}
}
