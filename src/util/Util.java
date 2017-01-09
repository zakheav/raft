package util;

import java.lang.reflect.Field;
import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public class Util {
	private static Unsafe THE_UNSAFE = null;
	static {
		try {
			Field theUnsafe = null;
			theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
			theUnsafe.setAccessible(true); 
			THE_UNSAFE = (Unsafe) theUnsafe.get(null);
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	public static Unsafe get_unsafe() {
		return THE_UNSAFE;
	}
}
