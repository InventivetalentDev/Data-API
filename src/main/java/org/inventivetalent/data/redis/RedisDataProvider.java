package org.inventivetalent.data.redis;

import org.inventivetalent.data.async.AbstractAsyncDataProvider;
import org.inventivetalent.data.async.DataCallable;
import org.inventivetalent.data.async.DataCallback;
import redis.clients.jedis.Jedis;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

public class RedisDataProvider extends AbstractAsyncDataProvider<String> {

	private final Jedis  jedis;
	private final String keyFormat;
	private final String keyPattern;

	public RedisDataProvider(Jedis jedis) {
		this.jedis = jedis;
		this.keyFormat = "data-api:%s";
		this.keyPattern = "data-api:(.)";
	}

	public RedisDataProvider(Executor executor, Jedis jedis) {
		super(executor);
		this.jedis = jedis;
		this.keyFormat = "data-api:%s";
		this.keyPattern = "data-api:(.)";
	}

	public RedisDataProvider(Jedis jedis, String keyFormat, String keyPattern) {
		this.jedis = jedis;
		this.keyFormat = keyFormat;
		this.keyPattern = keyPattern;
	}

	public RedisDataProvider(Executor executor, Jedis jedis, String keyFormat, String keyPattern) {
		super(executor);
		this.jedis = jedis;
		this.keyFormat = keyFormat;
		this.keyPattern = keyPattern;
	}

	protected String formatKey(String key) {
		return String.format(this.keyFormat, key);
	}

	protected String extractKey(String key) {
		return key.replaceAll(this.keyPattern, "$1");
	}

	@Override
	public void put(@Nonnull String key, @Nonnull String value) {
		execute(() -> jedis.set(formatKey(key), value));
	}

	@Override
	public void put(@Nonnull String key, @Nonnull DataCallable<String> valueCallable) {
		execute(() -> jedis.set(formatKey(key), valueCallable.provide()));
	}

	@Override
	public void putAll(@Nonnull Map<String, String> map) {
		String[] keysValues = createKeysValues(map);
		execute(() -> jedis.mset(keysValues));
	}

	@Override
	public void putAll(@Nonnull DataCallable<Map<String, String>> mapCallable) {
		execute(() -> {
			Map<String, String> map = mapCallable.provide();
			String[] keysValues = createKeysValues(map);
			jedis.mset(keysValues);
		});
	}

	String[] createKeysValues(Map<String, String> map) {
		String[] keysValues = new String[map.size() * 2];
		int i = 0;
		for (Map.Entry<String, String> entry : map.entrySet()) {
			keysValues[i++] = formatKey(entry.getKey());
			keysValues[i++] = entry.getValue();
		}
		return keysValues;
	}

	@Override
	public void get(@Nonnull String key, @Nonnull DataCallback<String> callback) {
		execute(() -> callback.provide(jedis.get(formatKey(key))));
	}

	@Override
	public void contains(@Nonnull String key, @Nonnull DataCallback<Boolean> callback) {
		execute(() -> callback.provide(jedis.exists(formatKey(key))));
	}

	@Override
	public void remove(@Nonnull String key, @Nonnull DataCallback<String> callback) {
		execute(() -> {
			String formattedKey = formatKey(key);
			String value = jedis.get(formattedKey);
			jedis.del(formattedKey);
			callback.provide(value);
		});
	}

	@Override
	public void remove(@Nonnull String key) {
		execute(() -> jedis.del(formatKey(key)));
	}

	@Override
	public void keys(@Nonnull DataCallback<Collection<String>> callback) {
		execute(() -> {
			Set<String> rawKeys = jedis.keys(formatKey("*"));
			Set<String> keys = rawKeys.stream().map(this::extractKey).collect(Collectors.toSet());
			callback.provide(keys);
		});
	}

	@Override
	public void entries(@Nonnull DataCallback<Map<String, String>> callback) {
		execute(() -> {
			//TODO: make this more efficient
			Set<String> rawKeys = jedis.keys(formatKey("*"));
			List<String> keys = rawKeys.stream().map(this::extractKey).collect(Collectors.toList());
			List<String> values = jedis.mget(keys.toArray(new String[keys.size()]));
			Map<String, String> map = new HashMap<>();
			for (int i = 0; i < keys.size(); i++) {
				map.put(keys.get(i), values.get(i));
			}
			callback.provide(map);
		});
	}

	@Override
	public void size(@Nonnull DataCallback<Integer> callback) {
		execute(() -> {
			Set<String> rawKeys = jedis.keys(formatKey("*"));
			callback.provide(rawKeys.size());
		});
	}
}
