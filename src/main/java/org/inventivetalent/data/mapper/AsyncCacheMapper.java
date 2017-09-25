package org.inventivetalent.data.mapper;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.NonNull;
import org.inventivetalent.data.DataProvider;
import org.inventivetalent.data.async.AsyncDataProvider;
import org.inventivetalent.data.async.DataCallable;
import org.inventivetalent.data.async.DataCallback;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

@SuppressWarnings({"unused", "WeakerAccess"})
public class AsyncCacheMapper {

	public static <V> CachedDataProvider<V> create(AsyncDataProvider<V> provider) {
		return create(provider, CacheBuilder.newBuilder());
	}

	public static <V> CachedDataProvider<V> create(AsyncDataProvider<V> provider, CacheBuilder<Object, Object> cacheBuilder) {
		return create(provider, cacheBuilder, Executors.newSingleThreadExecutor());
	}

	@SuppressWarnings("unchecked")
	public static <V> CachedDataProvider<V> create(AsyncDataProvider<V> provider, CacheBuilder<Object, Object> cacheBuilder, Executor cacheExecutor) {
		LoadingCache<String, Optional<V>> cache = cacheBuilder.build(CacheLoader.asyncReloading(new CacheLoader<String, java.util.Optional<V>>() {
			@Override
			public java.util.Optional<V> load(String key) throws Exception {
				final Object[] value = new Object[1];
				CountDownLatch latch = new CountDownLatch(1);
				provider.get(key, v -> {
					value[0] = v;
					latch.countDown();
				});
				latch.await(10, TimeUnit.SECONDS);
				return Optional.ofNullable((V) value[0]);
			}
		}, cacheExecutor));
		return new CachedDataProvider<>(provider, cache, cacheExecutor);
	}

	public static class CachedDataProvider<V> implements AsyncDataProvider<V>, DataProvider<V> {

		AsyncDataProvider<V> provider;
		LoadingCache<String, Optional<V>> cache;
		Executor cacheExecutor;

		CachedDataProvider(AsyncDataProvider<V> provider, LoadingCache<String, Optional<V>> cache, Executor cacheExecutor) {
			this.provider = provider;
			this.cache = cache;
			this.cacheExecutor = cacheExecutor;
		}

		@Override
		public void put(@NonNull String key, @NonNull V value) {
			cache.put(key, Optional.of(value));
			provider.put(key, value);
		}

		@Override
		public void put(@NonNull String key, @NonNull DataCallable<V> valueCallable) {
			provider.execute(() -> {
				V value = valueCallable.provide();
				cache.put(key, Optional.of(value));
				provider.put(key, value);
			});
		}

		@Override
		public void putAll(@NonNull Map<String, V> map) {
			Map<String, Optional<V>> optionalMap = new HashMap<>();
			for (Map.Entry<String, V> entry : map.entrySet()) {
				optionalMap.put(entry.getKey(), Optional.ofNullable(entry.getValue()));
			}
			cache.putAll(optionalMap);
		}

		@Override
		public void putAll(@NonNull DataCallable<Map<String, V>> mapCallable) {
			cacheExecutor.execute(() -> {
				Map<String, V> map = mapCallable.provide();
				Map<String, Optional<V>> optionalMap = new HashMap<>();
				for (Map.Entry<String, V> entry : map.entrySet()) {
					optionalMap.put(entry.getKey(), Optional.ofNullable(entry.getValue()));
				}
				cache.putAll(optionalMap);
			});
		}

		@Override
		public void get(@NonNull String key, @NonNull DataCallback<V> callback) {
			cacheExecutor.execute(() -> {
				try {
					callback.provide(cache.get(key).orElse(null));
				} catch (ExecutionException e) {
					throw new RuntimeException(e);
				}
			});
		}

		@Override
		public V get(@NonNull String key) {
			Optional<V> value = cache.getIfPresent(key);
			return value != null ? value.orElse(null) : null;
		}

		@Override
		public void contains(@NonNull String key, @NonNull DataCallback<Boolean> callback) {
			provider.contains(key, callback);
		}

		@Override
		public boolean contains(@NonNull String key) {
			Optional<V> value = cache.getIfPresent(key);
			return value != null && value.isPresent();
		}

		@Override
		public void remove(@NonNull String key, @NonNull DataCallback<V> callback) {
			cache.invalidate(key);
			provider.remove(key, callback);
		}

		@Override
		public void remove(@NonNull String key) {
			cache.invalidate(key);
			provider.remove(key);
		}

		@Override
		public V getAndRemove(@NonNull String key) {
			Optional<V> value = cache.getIfPresent(key);
			cache.invalidate(key);
			provider.remove(key);
			return value != null ? value.orElse(null) : null;
		}

		@Override
		public void keys(@NonNull DataCallback<Collection<String>> callback) {
			provider.keys(callback);
		}

		@NonNull
		@Override
		public Collection<String> keys() {
			return cache.asMap().keySet();
		}

		@Override
		public void entries(@NonNull DataCallback<Map<String, V>> callback) {
			provider.entries(callback);
		}

		@NonNull
		@Override
		public Map<String, V> entries() {
			Map<String, Optional<V>> optionalMap = cache.asMap();
			Map<String, V> map = new HashMap<>();
			for (Map.Entry<String, Optional<V>> entry : optionalMap.entrySet()) {
				map.put(entry.getKey(), entry.getValue().orElse(null));
			}
			return map;
		}

		@Override
		public void size(@NonNull DataCallback<Integer> callback) {
			provider.size(callback);
		}

		@Override
		public int size() {
			return (int) cache.size();
		}
	}

}
