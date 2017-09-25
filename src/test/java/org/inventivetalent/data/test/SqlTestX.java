package org.inventivetalent.data.test;

import org.inventivetalent.data.async.AsyncDataProvider;
import org.inventivetalent.data.mapper.AsyncCacheMapper;
import org.inventivetalent.data.mapper.AsyncStringValueMapper;
import org.inventivetalent.data.sql.SQLDataProvider;
import org.junit.Test;

import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.*;


public class SqlTestX extends AbstractKeyValueTest {

	private SQLDataProvider provider;

	public SqlTestX() throws SQLException {
		super();
		this.provider = new SQLDataProvider("jdbc:mysql://localhost:3306/minecraft", "root", "admin", "test_table");
	}

	@Test
	public void putTest() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(keys.size());

		for (int i = 0; i < keys.size(); i++) {
			final int finalI = i;
			provider.put(keys.get(i), () -> {
				latch.countDown();
				return values.get(finalI);
			});
		}

		latch.await();
	}

	@Test
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
		AsyncDataProvider<String> stringProvider = AsyncStringValueMapper.sql(this.provider);

		CountDownLatch latch = new CountDownLatch(2);

		stringProvider.put("foo", () -> {
			latch.countDown();
			return "bar";
		});
		stringProvider.put("foo1", () -> {
			latch.countDown();
			return "bar1";
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
		AsyncCacheMapper.CachedDataProvider<String> cache = AsyncCacheMapper.create(AsyncStringValueMapper.sql(this.provider));

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
