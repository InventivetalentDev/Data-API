package org.inventivetalent.data.async;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;

public interface AsyncDataProvider<V> {

	void put(@Nonnull String key, @Nonnull V value);

	void put(@Nonnull String key, @Nonnull DataCallable<V> valueCallable);

	void putAll(@Nonnull Map<String, V> map);

	void putAll(@Nonnull DataCallable<Map<String, V>> mapCallable);

	void get(@Nonnull String key, @Nonnull DataCallback<V> callback);

	void contains(@Nonnull String key, @Nonnull DataCallback<Boolean> callback);

	void remove(@Nonnull String key, @Nonnull DataCallback<V> callback);

	void remove(@Nonnull String key);

	void keys(@Nonnull DataCallback<Collection<String>> callback);

	void entries(@Nonnull DataCallback<Map<String, V>> callback);

	void size(@Nonnull DataCallback<Integer> callback);

}
