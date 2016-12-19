package communicationUnit;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import util.XML;

public class ThreadPool {
	// 单例
	private static ThreadPool instance = new ThreadPool();

	@SuppressWarnings("unchecked")
	private ThreadPool() {
		Map<String, Object> conf = new XML().nodeConf();
		int nodeNum = ((List<String>) (conf.get("ipport"))).size();
		this.COMMONSIZE = nodeNum * 2;
		this.MAXSIZE = this.COMMONSIZE + 10;
		pool = new ArrayList<Thread>();
		tasks = new LinkedList<Runnable>();
		for (int i = 0; i < COMMONSIZE; ++i) {
			add_labour(new Worker());
		}
		System.out.println("线程池启动");
	}

	public static ThreadPool get_instance() {
		return instance;
	}// 单例

	private final int COMMONSIZE;
	private final int MAXSIZE;// 线程数量应大于节点数量
	private final int TASK_CRITICAL_SIZE = 100;
	private final ArrayList<Thread> pool;
	private final Queue<Runnable> tasks;

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
			remove_labour(this);
		}
	}

	public void add_labour(Thread t) {
		synchronized (this.pool) {
			if (pool.size() < MAXSIZE) {
				pool.add(t);
				pool.get(pool.size() - 1).start();
			}
		}
	}

	public void remove_labour(Thread t) {
		synchronized (this.pool) {
			if (!pool.isEmpty()) {
				pool.remove(t);
			}
		}
	}

	public void add_tasks(Runnable task) {
		synchronized (tasks) {
			tasks.offer(task);
			if (tasks.size() >= this.TASK_CRITICAL_SIZE) {
				add_labour(new CasualLaborer());
			}
			tasks.notify();
		}
	}
}