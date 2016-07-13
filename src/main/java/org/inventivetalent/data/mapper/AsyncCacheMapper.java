package org.inventivetalent.data.mapper;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.inventivetalent.data.DataProvider;
import org.inventivetalent.data.async.AsyncDataProvider;
import org.inventivetalent.data.async.DataCallable;
import org.inventivetalent.data.async.DataCallback;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;

public class AsyncCacheMapper {

	public static <V> CachedDataProvider<V> create(AsyncDataProvider<V> provider) {
		return create(provider, CacheBuilder.newBuilder());
	}

	public static <V> CachedDataProvider<V> create(AsyncDataProvider<V> provider, CacheBuilder<Object, Object> cacheBuilder) {
		return create(provider, cacheBuilder, Executors.newSingleThreadExecutor());
	}

	public static <V> CachedDataProvider<V> create(AsyncDataProvider<V> provider, CacheBuilder<Object, Object> cacheBuilder, Executor cacheExecutor) {
		LoadingCache<String, V> cache = cacheBuilder.build(CacheLoader.asyncReloading(new CacheLoader<String, V>() {
			@Override
			public V load(String key) throws Exception {
				final Object[] value = new Object[1];
				CountDownLatch latch = new CountDownLatch(1);
				provider.get(key, v -> {
					value[0] = v;
					latch.countDown();
				});
				latch.await(10, TimeUnit.SECONDS);
				return (V) value[0];
			}
		}, cacheExecutor));
		return new CachedDataProvider<>(provider, cache, cacheExecutor);
	}

	public static class CachedDataProvider<V> implements AsyncDataProvider<V>, DataProvider<V> {

		AsyncDataProvider<V>    provider;
		LoadingCache<String, V> cache;
		Executor                cacheExecutor;

		CachedDataProvider(AsyncDataProvider<V> provider, LoadingCache<String, V> cache, Executor cacheExecutor) {
			this.provider = provider;
			this.cache = cache;
			this.cacheExecutor = cacheExecutor;
		}

		@Override
		public void put(@Nonnull String key, @Nonnull V value) {
			cache.put(key, value);
			provider.put(key, value);
		}

		@Override
		public void put(@Nonnull String key, @Nonnull DataCallable<V> valueCallable) {
			provider.execute(() -> {
				V value = valueCallable.provide();
				cache.put(key, value);
				provider.put(key, value);
			});
		}

		@Override
		public void putAll(@Nonnull Map<String, V> map) {
			cache.putAll(map);
		}

		@Override
		public void putAll(@Nonnull DataCallable<Map<String, V>> mapCallable) {
			cacheExecutor.execute(() -> cache.putAll(mapCallable.provide()));
		}

		@Override
		public void get(@Nonnull String key, @Nonnull DataCallback<V> callback) {
			cacheExecutor.execute(() -> {
				try {
					callback.provide(cache.get(key));
				} catch (ExecutionException e) {
					throw new RuntimeException(e);
				}
			});
		}

		@Nullable
		@Override
		public V get(@Nonnull String key) {
			return cache.getIfPresent(key);
		}

		@Override
		public void contains(@Nonnull String key, @Nonnull DataCallback<Boolean> callback) {
			provider.contains(key, callback);
		}

		@Override
		public boolean contains(@Nonnull String key) {
			return cache.getIfPresent(key) != null;
		}

		@Override
		public void remove(@Nonnull String key, @Nonnull DataCallback<V> callback) {
			cache.invalidate(key);
			provider.remove(key, callback);
		}

		@Override
		public void remove(@Nonnull String key) {
			cache.invalidate(key);
			provider.remove(key);
		}

		@Nullable
		@Override
		public V getAndRemove(@Nonnull String key) {
			V value = cache.getIfPresent(key);
			cache.invalidate(key);
			provider.remove(key);
			return value;
		}

		@Override
		public void keys(@Nonnull DataCallback<Collection<String>> callback) {
			provider.keys(callback);
		}

		@Nonnull
		@Override
		public Collection<String> keys() {
			return cache.asMap().keySet();
		}

		@Override
		public void entries(@Nonnull DataCallback<Map<String, V>> callback) {
			provider.entries(callback);
		}

		@Nonnull
		@Override
		public Map<String, V> entries() {
			return cache.asMap();
		}

		@Override
		public void size(@Nonnull DataCallback<Integer> callback) {
			provider.size(callback);
		}

		@Override
		public int size() {
			return (int) cache.size();
		}
	}

}
