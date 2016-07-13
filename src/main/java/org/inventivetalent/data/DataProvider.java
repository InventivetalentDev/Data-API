package org.inventivetalent.data;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;

public interface DataProvider<V> {

	void put(@Nonnull String key, @Nonnull V value);

	void putAll(@Nonnull Map<String, V> map);

	@Nullable
	V get(@Nonnull String key);

	boolean contains(@Nonnull String key);

	void remove(@Nonnull String key);

	@Nullable
	V getAndRemove(@Nonnull String key);

	@Nonnull
	Collection<String> keys();

	@Nonnull
	Map<String, V> entries();

	int size();

}
