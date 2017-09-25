package org.inventivetalent.data;

import lombok.NonNull;

import java.util.Collection;
import java.util.Map;

@SuppressWarnings({"unused", "WeakerAccess"})
public interface DataProvider<V> {

	void put(@NonNull String key, @NonNull V value);

	void putAll(@NonNull Map<String, V> map);

	V get(@NonNull String key);

	boolean contains(@NonNull String key);

	void remove(@NonNull String key);

	V getAndRemove(@NonNull String key);

	@NonNull
	Collection<String> keys();

	@NonNull
	Map<String, V> entries();

	int size();

}
