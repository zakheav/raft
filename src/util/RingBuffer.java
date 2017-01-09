package util;

public class RingBuffer {
	private volatile int writeFinish = 0;// 生产者线程向消费者线程发消息

	private volatile int getReadLock = 0;// 同步对readPtr的操作
	private volatile int getWriteLock = 0;// 同步对writePtr的操作

	private final int SIZE;
	private Object[] ringBuffer;
	public SequenceNum readPtr;// 可以读的第一个下标
	public SequenceNum writePtr;// 可以写的第一个下标

	private volatile boolean memoryBarrier = true;// 提供内存屏障支持
	@SuppressWarnings("unused")
	private volatile boolean mb = true;// 提供内存屏障支持

	// writePtr 在 readPtr后面
	// readPtr == writePtr时ringBuffer为空
	// readPtr == (writePtr+1)%size时ringBuffer为满

	// 下面的所有操作都不保证原子性

	public RingBuffer(int size) {
		this.SIZE = size;
		this.ringBuffer = new Object[size];
		this.readPtr = new SequenceNum();
		this.writePtr = new SequenceNum();
	}

	public boolean add_element(Object o) {// 会同步多个线程的同时写
		while (getWriteLock != 0)
			;
		getWriteLock = 1;// 获取写锁

		int readIdx = readPtr.get();
		int writeIdx = writePtr.get();
		
		if((writeIdx + 1) % SIZE == readIdx) {// buffer已经满了
			getWriteLock = 0;// 释放写锁
			return false;
		}
		
		writeFinish = 0;
		mb = memoryBarrier;// 内存屏障, 保证后面的指令不会重排序到这条指令之前
		
		ringBuffer[writeIdx] = o;
		writePtr.increase(SIZE);// 写元素
		
		writeFinish = 1;// 生产者写结束，向消费者发出消息

		getWriteLock = 0;// 释放写锁
		return true;
	}

	public Object get_element() {// 会同步多个线程的同时读
		while (getReadLock != 0)
			;
		getReadLock = 1;// 获取读锁
		
		int readIdx = readPtr.get();
		int writeIdx = writePtr.get();
		
		if(readIdx == writeIdx) {// buffer已经空了
			getReadLock = 0;// 释放读锁
			return null;
		}
		
		while (writeFinish == 0)
			;// 等待生产者写结束
		Object o = ringBuffer[readIdx];
		readPtr.increase(SIZE);// 读取元素
		
		getReadLock = 0;// 释放读锁
		return o;
	}

	public boolean isEmpty() {
		int readIdx = readPtr.get();
		int writeIdx = writePtr.get();
		return readIdx == writeIdx;
	}
}
