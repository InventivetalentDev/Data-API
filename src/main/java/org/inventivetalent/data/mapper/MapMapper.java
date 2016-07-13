package org.inventivetalent.data.mapper;

import org.inventivetalent.data.DataProvider;
import org.inventivetalent.data.async.AsyncDataProvider;
import org.inventivetalent.data.async.DataCallable;
import org.inventivetalent.data.async.DataCallback;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executor;

public class MapMapper {

	public static <V> AsyncDataProvider<V> async(Map<String, V> map) {
		return new AsyncDataProvider<V>() {

			@Override
			public void execute(Runnable runnable) {
				runnable.run();
			}

			@Override
			public Executor getExecutor() {
				return Runnable::run;
			}

			@Override
			public void put(@Nonnull String key, @Nonnull V value) {
				map.put(key, value);
			}

			@Override
			public void put(@Nonnull String key, @Nonnull DataCallable<V> valueCallable) {
				map.put(key, valueCallable.provide());
			}

			@Override
			public void putAll(@Nonnull Map<String, V> map0) {
				map.putAll(map0);
			}

			@Override
			public void putAll(@Nonnull DataCallable<Map<String, V>> mapCallable) {
				map.putAll(mapCallable.provide());
			}

			@Override
			public void get(@Nonnull String key, @Nonnull DataCallback<V> callback) {
				callback.provide(map.get(key));
			}

			@Override
			public void contains(@Nonnull String key, @Nonnull DataCallback<Boolean> callback) {
				callback.provide(map.containsKey(key));
			}

			@Override
			public void remove(@Nonnull String key, @Nonnull DataCallback<V> callback) {
				callback.provide(map.remove(key));
			}

			@Override
			public void remove(@Nonnull String key) {
				map.remove(key);
			}

			@Override
			public void keys(@Nonnull DataCallback<Collection<String>> callback) {
				callback.provide(map.keySet());
			}

			@Override
			public void entries(@Nonnull DataCallback<Map<String, V>> callback) {
				callback.provide(map);
			}

			@Override
			public void size(@Nonnull DataCallback<Integer> callback) {
				callback.provide(map.size());
			}
		};
	}

	public static <V> DataProvider<V> sync(Map<String, V> map) {
		return new DataProvider<V>() {
			@Override
			public void put(@Nonnull String key, @Nonnull V value) {
				map.put(key, value);
			}

			@Override
			public void putAll(@Nonnull Map<String, V> map0) {
map.putAll(map0);
			}

			@Nullable
			@Override
			public V get(@Nonnull String key) {
				return map.get(key);
			}

			@Override
			public boolean contains(@Nonnull String key) {
				return map.containsKey(key);
			}

			@Override
			public void remove(@Nonnull String key) {
				map.remove(key);
			}

			@Nullable
			@Override
			public V getAndRemove(@Nonnull String key) {
				return map.remove(key);
			}

			@Nonnull
			@Override
			public Collection<String> keys() {
				return map.keySet();
			}

			@Nonnull
			@Override
			public Map<String, V> entries() {
				return map;
			}

			@Override
			public int size() {
				return map.size();
			}
		};
	}

}
