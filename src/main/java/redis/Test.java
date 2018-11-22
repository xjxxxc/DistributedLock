package redis;

/**
* @author: xujunxian
*/
public class Test {
	/**
	 * 循环次数
	 */
	private static int count = 50;
    public static void main(String[] args) {
        Service service = new Service();
        for (int i = 0; i < count; i++) {
            ThreadA threadA = new ThreadA(service);
            threadA.start();
        }
    }
}
