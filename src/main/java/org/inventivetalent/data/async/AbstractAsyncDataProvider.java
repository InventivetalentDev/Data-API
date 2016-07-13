package org.inventivetalent.data.async;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public abstract class AbstractAsyncDataProvider<V> implements AsyncDataProvider<V> {

	private Executor executor;

	public AbstractAsyncDataProvider() {
		this.executor = Executors.newSingleThreadExecutor();
	}

	public AbstractAsyncDataProvider(Executor executor) {
		this.executor = executor;
	}

	@Override
	public void execute(Runnable runnable) {
		if (executor == null) {
			throw new IllegalStateException("Missing executor");
		}
		executor.execute(runnable);
	}

	@Override
	public Executor getExecutor() {
		return executor;
	}
}
