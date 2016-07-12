package org.inventivetalent.data.test;

import com.google.gson.JsonObject;
import org.inventivetalent.data.async.DataCallable;
import org.inventivetalent.data.mongodb.MongoDbDataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.concurrent.CountDownLatch;

import static org.testng.Assert.*;

public class MongoDbTest extends AbstractKeyValueTest {

	private MongoDbDataProvider provider;

	public MongoDbTest() {
		super();
		this.provider = new MongoDbDataProvider("192.168.178.34", 27017, "", new char[0], null, "data_test", "c1");
	}

	@Test(enabled = false)
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

	@Test(enabled = false,
		  dependsOnMethods = { "putTest" })
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

}
