package serverUnit;

public class LogReleaseThread implements Runnable {

	@Override
	public void run() {
		try {
			Thread.sleep(1000 * 60);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		Node.get_instance().server.log.logClear();
	}

}
