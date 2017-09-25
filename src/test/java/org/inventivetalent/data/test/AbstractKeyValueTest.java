package org.inventivetalent.data.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

abstract class AbstractKeyValueTest {

	List<String> keys = new ArrayList<>();
	List<String> values = new ArrayList<>();

	AbstractKeyValueTest() {
		Random random = new Random();
		for (int i = 0; i < 10; i++) {
			keys.add(String.valueOf(random.nextDouble()));
			values.add(String.valueOf(random.nextInt()));
		}
	}

}
