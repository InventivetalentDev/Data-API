package org.inventivetalent.data.test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class GsonTest {

	@Test
	public void equalityTest() {
		JsonObject original = new JsonObject();
		original.addProperty("foo", "bar");
		original.addProperty("foo1", "bar1");
		original.addProperty("foo2", "bar2");
		original.addProperty("foo3", "bar3");

		JsonObject copy = new Gson().fromJson(original, JsonObject.class);

		assertEquals(copy, original);
	}

}
