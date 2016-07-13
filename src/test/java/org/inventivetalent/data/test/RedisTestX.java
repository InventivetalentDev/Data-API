package org.inventivetalent.data.test;

import org.inventivetalent.data.async.AsyncDataProvider;
import org.inventivetalent.data.async.DataCallable;
import org.inventivetalent.data.mapper.AsyncCacheMapper;
import org.inventivetalent.data.mapper.AsyncStringValueMapper;
import org.inventivetalent.data.redis.RedisDataProvider;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;

import javax.annotation.Nonnull;
import java.util.concurrent.CountDownLatch;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

@Test
public class RedisTestX extends AbstractKeyValueTest {

	private RedisDataProvider provider;

	public RedisTestX() {
		super();
		Jedis jedis = new Jedis("192.168.178.34", 6379);
		jedis.connect();
		this.provider = new RedisDataProvider(jedis);
	}

	@Test
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

	@Test(dependsOnMethods = { "putTest" })
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

	@Test
	public void stringMapperTest() throws InterruptedException {
		AsyncDataProvider<String> stringProvider = AsyncStringValueMapper.redis(this.provider);

		CountDownLatch latch = new CountDownLatch(2);

		stringProvider.put("foo", new DataCallable<String>() {
			@Nonnull
			@Override
			public String provide() {
				latch.countDown();
				return "bar";
			}
		});
		stringProvider.put("foo1", new DataCallable<String>() {
			@Nonnull
			@Override
			public String provide() {
				latch.countDown();
				return "bar1";
			}
		});
		latch.await();

		CountDownLatch latch1 = new CountDownLatch(2);
		stringProvider.get("foo", s -> {
			assertNotNull(s);
			assertEquals(s, "bar");
			latch1.countDown();
		});
		stringProvider.get("foo1", s -> {
			assertNotNull(s);
			assertEquals(s, "bar1");
			latch1.countDown();
		});
	}

	@Test
	public void cacheTest() throws InterruptedException {
		AsyncCacheMapper.CachedDataProvider<String> cache = AsyncCacheMapper.create(AsyncStringValueMapper.redis(this.provider));

		assertNull(cache.get("foo"));// Should be null before it's cached

		CountDownLatch latch = new CountDownLatch(1);
		cache.get("foo", s -> {
			assertNotNull(s);
			latch.countDown();
		});
		latch.await();

		assertNotNull(cache.get("foo"));// Should exist after caching
	}


}
