package org.inventivetalent.data.test;

import com.google.gson.JsonObject;
import org.inventivetalent.data.async.AsyncDataProvider;
import org.inventivetalent.data.async.DataCallable;
import org.inventivetalent.data.mapper.AsyncStringValueMapper;
import org.inventivetalent.data.mongodb.MongoDbDataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.concurrent.CountDownLatch;

import static org.testng.Assert.*;

@Test(enabled = false)
public class MongoDbTest extends AbstractKeyValueTest {

	private MongoDbDataProvider provider;

	public MongoDbTest() {
		super();
		this.provider = new MongoDbDataProvider("192.168.178.34", 27017, "", new char[0], null, "data_test", "c1");
	}

	@Test
	public void putTest() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(keys.size());

		for (int i = 0; i < keys.size(); i++) {
			final int finalI = i;
			provider.put(keys.get(i), new DataCallable<JsonObject>() {
				@Nonnull
				@Override
				public JsonObject provide() {
					latch.countDown();
					JsonObject jsonObject = new JsonObject();
					jsonObject.addProperty("val", values.get(finalI));
					return jsonObject;
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
			provider.get(keys.get(i), jsonObject -> {
				assertNotNull(jsonObject);
				assertTrue(jsonObject.has("val"));
				assertEquals(jsonObject.get("val").getAsString(), values.get(finalI));
				latch.countDown();
			});
		}

		latch.await();
	}

	@Test
	public void stringMapperTest() throws InterruptedException {
		AsyncDataProvider<String> stringProvider = AsyncStringValueMapper.mongoDb(this.provider);

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

}
