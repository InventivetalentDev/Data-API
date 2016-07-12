package org.inventivetalent.data.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public abstract class AbstractKeyValueTest {

	protected List<String>     keys   = new ArrayList<>();
	protected List<String> values = new ArrayList<>();

	public AbstractKeyValueTest() {
		Random random = new Random();
		for (int i = 0; i < 10; i++) {
			keys.add(String.valueOf(random.nextDouble()));
			values.add(String.valueOf(random.nextInt()));
		}
	}


}
