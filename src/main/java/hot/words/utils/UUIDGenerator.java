package hot.words.utils;

import java.util.UUID;

/**
 * JSON Java对象转换
 * @author Daijiao
 * @version 1.0
 * 2015/09/14 added
 *
 */
public class UUIDGenerator {

	public UUIDGenerator(){}
	
	/**
	 * 获得一个UUID，去掉 "-" 
	 * @return
	 */
	public static String getUUID() {
		String str = UUID.randomUUID().toString();
		return str.replaceAll("-", "");
	}
	
	/**
	 * 获得唯一序列
	 * @return
	 */
	public static String getSequence() {
		
		return null;
	}
	
	/**
	 * 获得指定数目的一批UUID
	 * @param number
	 * @return
	 */
	public static String[] getUUID(int number) {
		if (number < 1) {
			return null;
		}
		String[] ss = new String[number];
		for (int i = 0; i < number; i++) {
			ss[i] = getUUID();
		}
		return ss;
	}
	
}
