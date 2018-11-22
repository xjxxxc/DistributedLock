package zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
* @author: xujunxian
*/
public class DistributedLock implements Lock, Watcher {
	private ZooKeeper zk = null;
	/**
	 * 根节点
	 */
	private String rootLock = "/locks";
	/**
	 *  竞争的资源
	 */
	private String lockName;
	/**
	 *  等待的前一个锁
	 */
	private String waitLock;
	/**
	 *  当前锁
	 */
	private String currentLock;
	/**
	 * 计数器
	 */
	private CountDownLatch countDownLatch;
	private int sessionTimeout = 30000;
	private List<Exception> exceptionList = new ArrayList<Exception>();

	/**
	 * 配置分布式锁
	 * @param config 连接的url
	 * @param lockName 竞争资源
	 */
	public DistributedLock(String config, String lockName) {
		this.lockName = lockName;
		try {
			// 连接zookeeper
			zk = new ZooKeeper(config, sessionTimeout, this);
			Stat stat = zk.exists(rootLock, false);
			if (stat == null) {
				// 如果根节点不存在，则创建根节点
				zk.create(rootLock, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}

	/**
	 *  节点监视器
	 */
	public void process(WatchedEvent event) {
		if (this.countDownLatch != null) {
			this.countDownLatch.countDown();
		}
	}

	public void lock() {
		if (exceptionList.size() > 0) {
			throw new LockException(exceptionList.get(0));
		}
		try {
			if (this.tryLock()) {
				System.out.println(Thread.currentThread().getName() + " " + lockName + "获得了锁");
				return;
			} else {
				// 等待锁
				waitForLock(waitLock, sessionTimeout);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}

	public boolean tryLock() {
		try {
			String splitStr = "_lock_";
			if (lockName.contains(splitStr)) {
				throw new LockException("锁名有误");
			}
			// 创建临时有序节点
			currentLock = zk.create(rootLock + "/" + lockName + splitStr, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL_SEQUENTIAL);
			System.out.println(currentLock + " 已经创建");
			// 取所有子节点
			List<String> subNodes = zk.getChildren(rootLock, false);
			// 取出所有lockName的锁
			List<String> lockObjects = new ArrayList<String>();
			for (String node : subNodes) {
				String nodeTmp = node.split(splitStr)[0];
				if (nodeTmp.equals(lockName)) {
					lockObjects.add(node);
				}
			}
			Collections.sort(lockObjects);
			System.out.println(Thread.currentThread().getName() + " 的锁是 " + currentLock);
			// 若当前节点为最小节点，则获取锁成功
			if (currentLock.equals(rootLock + "/" + lockObjects.get(0))) {
				return true;
			}

			// 若不是最小节点，则找到自己的前一个节点
			String prevNode = currentLock.substring(currentLock.lastIndexOf("/") + 1);
			waitLock = lockObjects.get(Collections.binarySearch(lockObjects, prevNode) - 1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
		return false;
	}

	public boolean tryLock(long timeout, TimeUnit unit) {
		try {
			if (this.tryLock()) {
				return true;
			}
			return waitForLock(waitLock, timeout);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 *  等待锁
	 * @param prev
	 * @param waitTime
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private boolean waitForLock(String prev, long waitTime) throws KeeperException, InterruptedException {
		Stat stat = zk.exists(rootLock + "/" + prev, true);

		if (stat != null) {
			System.out.println(Thread.currentThread().getName() + "等待锁 " + rootLock + "/" + prev);
			this.countDownLatch = new CountDownLatch(1);
			// 计数等待，若等到前一个节点消失，则precess中进行countDown，停止等待，获取锁
			this.countDownLatch.await(waitTime, TimeUnit.MILLISECONDS);
			this.countDownLatch = null;
			System.out.println(Thread.currentThread().getName() + " 等到了锁");
		}
		return true;
	}

	public void unlock() {
		try {
			System.out.println("释放锁 " + currentLock);
			zk.delete(currentLock, -1);
			currentLock = null;
			zk.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}

	public Condition newCondition() {
		return null;
	}

	public void lockInterruptibly() throws InterruptedException {
		this.lock();
	}

	public class LockException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public LockException(String e) {
			super(e);
		}

		public LockException(Exception e) {
			super(e);
		}
	}
}
