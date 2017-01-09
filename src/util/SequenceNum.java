package util;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public class SequenceNum {
	private static final Unsafe unsafe;
	@SuppressWarnings("unused")
	private int p1, p2, p3, p4, p5, p6, p7;// 缓冲行填充
	private volatile int value;
	@SuppressWarnings("unused")
	private long p8, p9, p10, p11, p12, p13, p14;// 缓冲行填充
	private static final long VALUE_OFFSET;

	static {
		unsafe = Util.get_unsafe();
		try {
			VALUE_OFFSET = unsafe.objectFieldOffset(SequenceNum.class.getDeclaredField("value"));
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public SequenceNum() {
		value = 0;
	}

	public boolean compareAndSet(final int expectedValue, final int newValue) {
		return unsafe.compareAndSwapInt(this, VALUE_OFFSET, expectedValue, newValue);
	}

	public int get() {
		return value;
	}

	public int increase(int size) {
		while (true) {
			int now = get();
			if (compareAndSet(now, (now + 1) % size)) {
				return now + 1;
			}
		}
	}

	public int increase() {
		while (true) {
			int now = get();
			if (compareAndSet(now, now + 1)) {
				return now + 1;
			}
		}
	}
	
	public int decrease() {
		while (true) {
			int now = get();
			if (compareAndSet(now, now - 1)) {
				return now - 1;
			}
		}
	}
}
