package timerUnit;

import java.util.ArrayList;
import java.util.List;
import communicationUnit.Massage;
import communicationUnit.MassageQueue;
import util.JSON;

public class TimerThread implements Runnable {
	// singleton
	private static TimerThread instance = new TimerThread();

	private TimerThread() {
		this.timer = (int) (Math.random() * 50) + 15;
		this.timerCopy = timer;
		this.leaderTimerCopy = (int) (Math.random() * 10) + 5;
	}

	public static TimerThread get_instance() {
		return instance;
	}// singleton

	private int timer;
	private int timerCopy;
	private int leaderTimerCopy;

	public void reset_timer() {
		timer = timerCopy;
		System.out.println("reset timer, "+timer);
	}

	public void set_timer() {
		this.timer = (int) (Math.random() * 50) + 15;;
		this.timerCopy = this.timer;
	}

	public void reset_leaderTimer() {
		this.timer = leaderTimerCopy;
		System.out.println("reset leader timer, "+timer);
	}

	@Override
	public void run() {
		while (true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (timer == 0) {
				// append timeout massage into MassageQueue
				List<Object> msg4 = new ArrayList<Object>();
				int type = 4;
				msg4.add(type);
				Massage massage = new Massage(null, JSON.ArrayToJSON(msg4));
				MassageQueue.get_instance().add_massage(massage);
			} else {
				--timer;
			}
		}
	}

}
