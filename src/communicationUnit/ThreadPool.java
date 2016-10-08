package communicationUnit;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

public class ThreadPool {
	// 单例
	private static ThreadPool instance = new ThreadPool();

	private ThreadPool() {
		pool = new ArrayList<Thread>();
		tasks = new LinkedList<Runnable>();
		for (int i = 0; i < COMMONSIZE; ++i) {
			addLabour(new Worker());
		}
		System.out.println("线程池启动");
	}

	public static ThreadPool getInstance() {
		return instance;
	}// 单例

	private int COMMONSIZE = 20;
	private int MAXSIZE = 40;// 线程数量应大于节点数量
	private int TASK_CRITICAL_SIZE = 100;
	private ArrayList<Thread> pool;
	private Queue<Runnable> tasks;

	class Worker extends Thread {
		public void run() {
			while (true) {
				Runnable task;
				synchronized (tasks) {
					while (tasks.isEmpty()) {
						try {
							tasks.wait();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					task = tasks.poll();
				}
				task.run();
			}
		}
	}

	class CasualLaborer extends Thread {
		public void run() {
			while (true) {
				Runnable task = null;
				synchronized (tasks) {
					if (!tasks.isEmpty()) {
						task = tasks.poll();
					}
				}
				if (task != null) {
					task.run();
				} else {
					break;
				}
			}
			removeLabour(this);
		}
	}

	public void addLabour(Thread t) {
		synchronized (this.pool) {
			if (pool.size() < MAXSIZE) {
				pool.add(t);
				pool.get(pool.size() - 1).start();
			}
		}
	}

	public void removeLabour(Thread t) {
		synchronized (this.pool) {
			if (!pool.isEmpty()) {
				pool.remove(t);
			}
		}
	}

	public void addTasks(Runnable task) {
		synchronized (tasks) {
			tasks.offer(task);
			if (tasks.size() >= this.TASK_CRITICAL_SIZE) {
				addLabour(new CasualLaborer());
			}
			tasks.notify();
		}
	}
}