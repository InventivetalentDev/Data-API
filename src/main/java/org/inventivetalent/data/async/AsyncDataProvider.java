package org.inventivetalent.data.async;

import lombok.NonNull;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executor;

public interface AsyncDataProvider<V> {

	void put(@NonNull String key, @NonNull V value);

	void put(@NonNull String key, @NonNull DataCallable<V> valueCallable);

	void putAll(@NonNull Map<String, V> map);

	void putAll(@NonNull DataCallable<Map<String, V>> mapCallable);

	void get(@NonNull String key, @NonNull DataCallback<V> callback);

	void contains(@NonNull String key, @NonNull DataCallback<Boolean> callback);

	void remove(@NonNull String key, @NonNull DataCallback<V> callback);

	void remove(@NonNull String key);

	void keys(@NonNull DataCallback<Collection<String>> callback);

	void entries(@NonNull DataCallback<Map<String, V>> callback);

	void size(@NonNull DataCallback<Integer> callback);

	default void execute(Runnable runnable) {
		getExecutor().execute(runnable);
	}

	default Executor getExecutor() {
		return null;
	}
}
