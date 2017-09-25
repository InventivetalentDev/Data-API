package org.inventivetalent.data.mapper;

import lombok.NonNull;
import org.inventivetalent.data.DataProvider;
import org.inventivetalent.data.async.AsyncDataProvider;
import org.inventivetalent.data.async.DataCallable;
import org.inventivetalent.data.async.DataCallback;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executor;

@SuppressWarnings({"unused", "WeakerAccess"})
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
			public void put(@NonNull String key, @NonNull V value) {
				map.put(key, value);
			}

			@Override
			public void put(@NonNull String key, @NonNull DataCallable<V> valueCallable) {
				map.put(key, valueCallable.provide());
			}

			@Override
			public void putAll(@NonNull Map<String, V> map0) {
				map.putAll(map0);
			}

			@Override
			public void putAll(@NonNull DataCallable<Map<String, V>> mapCallable) {
				map.putAll(mapCallable.provide());
			}

			@Override
			public void get(@NonNull String key, @NonNull DataCallback<V> callback) {
				callback.provide(map.get(key));
			}

			@Override
			public void contains(@NonNull String key, @NonNull DataCallback<Boolean> callback) {
				callback.provide(map.containsKey(key));
			}

			@Override
			public void remove(@NonNull String key, @NonNull DataCallback<V> callback) {
				callback.provide(map.remove(key));
			}

			@Override
			public void remove(@NonNull String key) {
				map.remove(key);
			}

			@Override
			public void keys(@NonNull DataCallback<Collection<String>> callback) {
				callback.provide(map.keySet());
			}

			@Override
			public void entries(@NonNull DataCallback<Map<String, V>> callback) {
				callback.provide(map);
			}

			@Override
			public void size(@NonNull DataCallback<Integer> callback) {
				callback.provide(map.size());
			}
		};
	}

	public static <V> DataProvider<V> sync(Map<String, V> map) {
		return new DataProvider<V>() {
			@Override
			public void put(@NonNull String key, @NonNull V value) {
				map.put(key, value);
			}

			@Override
			public void putAll(@NonNull Map<String, V> map0) {
				map.putAll(map0);
			}

			@Override
			public V get(@NonNull String key) {
				return map.get(key);
			}

			@Override
			public boolean contains(@NonNull String key) {
				return map.containsKey(key);
			}

			@Override
			public void remove(@NonNull String key) {
				map.remove(key);
			}

			@Override
			public V getAndRemove(@NonNull String key) {
				return map.remove(key);
			}

			@NonNull
			@Override
			public Collection<String> keys() {
				return map.keySet();
			}

			@NonNull
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
