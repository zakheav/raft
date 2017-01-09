package communicationUnit;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import util.XML;

public class ThreadPool {
	private static ThreadPool instance = new ThreadPool();

	@SuppressWarnings("unchecked")
	private ThreadPool() {
		Map<String, Object> conf = new XML().nodeConf();
		int nodeNum = ((List<String>) (conf.get("ipport"))).size();
		this.POOLSIZE = nodeNum * 50;
		pool = new ArrayList<Thread>();
		tasks = new LinkedList<Runnable>();
		for (int i = 0; i < POOLSIZE; ++i) {
			add_labour(new Worker());
		}
		System.out.println("thread pool start");
	}

	public static ThreadPool get_instance() {
		return instance;
	}

	private final int POOLSIZE;
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
							e.printStackTrace();
						}
					}
					task = tasks.poll();
				}
				task.run();
			}
		}
	}

	public void add_labour(Thread t) {
		synchronized (this.pool) {
			if (pool.size() < POOLSIZE) {
				pool.add(t);
				pool.get(pool.size() - 1).start();
			}
		}
	}

	public void add_tasks(Runnable task) {
		synchronized (tasks) {
			tasks.offer(task);
			tasks.notify();
		}
	}
}