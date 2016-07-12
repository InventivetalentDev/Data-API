package org.inventivetalent.data.test;

import org.inventivetalent.data.async.DataCallable;
import org.inventivetalent.data.redis.RedisDataProvider;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;

import javax.annotation.Nonnull;
import java.util.concurrent.CountDownLatch;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class RedisTest extends AbstractKeyValueTest {

	private RedisDataProvider provider;

	public RedisTest() {
		super();
		Jedis jedis = new Jedis("192.168.178.34", 6379);
		jedis.connect();
		this.provider = new RedisDataProvider(jedis);
	}

	@Test(enabled = false)
	public void putTest() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(keys.size());

		for (int i = 0; i < keys.size(); i++) {
			final int finalI = i;
			provider.put(keys.get(i), new DataCallable<String>() {
				@Nonnull
				@Override
				public String provide() {
					latch.countDown();
					return values.get(finalI);
				}
			});
		}

		latch.await();
	}

	@Test(enabled = false,
		  dependsOnMethods = { "putTest" })
	public void getTest() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(keys.size());

		for (int i = 0; i < keys.size(); i++) {
			final int finalI = i;
			provider.get(keys.get(i), string -> {
				assertNotNull(string);
				assertEquals(string, values.get(finalI));
				latch.countDown();
			});
		}

		latch.await();
	}

}
