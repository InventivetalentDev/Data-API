package org.inventivetalent.data.mapper;

import com.google.gson.JsonObject;
import org.inventivetalent.data.async.AsyncDataProvider;
import org.inventivetalent.data.async.DataCallable;
import org.inventivetalent.data.async.DataCallback;
import org.inventivetalent.data.ebean.EbeanDataProvider;
import org.inventivetalent.data.ebean.KeyValueBean;
import org.inventivetalent.data.mongodb.MongoDbDataProvider;
import org.inventivetalent.data.redis.RedisDataProvider;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public abstract class AsyncStringValueMapper {

	public static AsyncDataProvider<String> redis(RedisDataProvider provider) {
		return provider;
	}

	public static AsyncDataProvider<String> mongoDb(MongoDbDataProvider provider) {
		return new AsyncDataProvider<String>() {

			public JsonObject makeValue(String value) {
				JsonObject jsonObject = new JsonObject();
				jsonObject.addProperty("value", value);
				return jsonObject;
			}

			public String getValue(JsonObject jsonObject) {
				return jsonObject.get("value").getAsString();
			}

			@Override
			public void put(@Nonnull String key, @Nonnull String value) {
				provider.put(key, makeValue(value));
			}

			@Override
			public void put(@Nonnull String key, @Nonnull DataCallable<String> valueCallable) {
				provider.put(key, new DataCallable<JsonObject>() {
					@Nonnull
					@Override
					public JsonObject provide() {
						return makeValue(valueCallable.provide());
					}
				});
			}

			@Override
			public void putAll(@Nonnull Map<String, String> map) {
				Map<String, JsonObject> jsonMap = new HashMap<>();
				for (Map.Entry<String, String> entry : map.entrySet()) {
					jsonMap.put(entry.getKey(), makeValue(entry.getValue()));
				}
				provider.putAll(jsonMap);
			}

			@Override
			public void putAll(@Nonnull DataCallable<Map<String, String>> mapCallable) {
				provider.putAll(new DataCallable<Map<String, JsonObject>>() {
					@Nonnull
					@Override
					public Map<String, JsonObject> provide() {
						Map<String, String> map = mapCallable.provide();
						Map<String, JsonObject> jsonMap = new HashMap<>();
						for (Map.Entry<String, String> entry : map.entrySet()) {
							jsonMap.put(entry.getKey(), makeValue(entry.getValue()));
						}
						return jsonMap;
					}
				});
			}

			@Override
			public void get(@Nonnull String key, @Nonnull DataCallback<String> callback) {
				provider.get(key, jsonObject -> callback.provide(getValue(jsonObject)));
			}

			@Override
			public void contains(@Nonnull String key, @Nonnull DataCallback<Boolean> callback) {
				provider.contains(key, callback);
			}

			@Override
			public void remove(@Nonnull String key, @Nonnull DataCallback<String> callback) {
				provider.remove(key, jsonObject -> callback.provide(getValue(jsonObject)));
			}

			@Override
			public void remove(@Nonnull String key) {
				provider.remove(key);
			}

			@Override
			public void keys(@Nonnull DataCallback<Collection<String>> callback) {
				provider.keys(callback);
			}

			@Override
			public void entries(@Nonnull DataCallback<Map<String, String>> callback) {
				provider.entries(stringJsonObjectMap -> {
					Map<String, String> stringMap = new HashMap<>();
					if (stringJsonObjectMap == null) {
						callback.provide(stringMap);
						return;
					}
					for (Map.Entry<String, JsonObject> entry : stringJsonObjectMap.entrySet()) {
						stringMap.put(entry.getKey(), getValue(entry.getValue()));
					}
					callback.provide(stringMap);
				});
			}

			@Override
			public void size(@Nonnull DataCallback<Integer> callback) {
				provider.size(callback);
			}
		};
	}

	public static AsyncDataProvider<String> ebean(EbeanDataProvider<KeyValueBean> provider) {
		return new AsyncDataProvider<String>() {
			@Override
			public void put(@Nonnull String key, @Nonnull String value) {
				provider.put(key, new KeyValueBean(key, value));
			}

			@Override
			public void put(@Nonnull String key, @Nonnull DataCallable<String> valueCallable) {
				provider.put(key, new DataCallable<KeyValueBean>() {
					@Nonnull
					@Override
					public KeyValueBean provide() {
						return new KeyValueBean(key, valueCallable.provide());
					}
				});
			}

			@Override
			public void putAll(@Nonnull Map<String, String> map) {
				Map<String, KeyValueBean> beanMap = new HashMap<>();
				for (Map.Entry<String, String> entry : map.entrySet()) {
					beanMap.put(entry.getKey(), new KeyValueBean(entry.getKey(), entry.getValue()));
				}
				provider.putAll(beanMap);
			}

			@Override
			public void putAll(@Nonnull DataCallable<Map<String, String>> mapCallable) {
				provider.putAll(new DataCallable<Map<String, KeyValueBean>>() {
					@Nonnull
					@Override
					public Map<String, KeyValueBean> provide() {
						Map<String, String> map = mapCallable.provide();
						Map<String, KeyValueBean> beanMap = new HashMap<>();
						for (Map.Entry<String, String> entry : map.entrySet()) {
							beanMap.put(entry.getKey(), new KeyValueBean(entry.getKey(), entry.getValue()));
						}
						return beanMap;
					}
				});
			}

			@Override
			public void get(@Nonnull String key, @Nonnull DataCallback<String> callback) {
				provider.get(key, keyValueBean -> callback.provide(keyValueBean != null ? keyValueBean.getValue() : null));
			}

			@Override
			public void contains(@Nonnull String key, @Nonnull DataCallback<Boolean> callback) {
				provider.contains(key, callback);
			}

			@Override
			public void remove(@Nonnull String key, @Nonnull DataCallback<String> callback) {
				provider.remove(key, keyValueBean -> callback.provide(keyValueBean != null ? keyValueBean.getValue() : null));
			}

			@Override
			public void remove(@Nonnull String key) {
				provider.remove(key);
			}

			@Override
			public void keys(@Nonnull DataCallback<Collection<String>> callback) {
				provider.keys(callback);
			}

			@Override
			public void entries(@Nonnull DataCallback<Map<String, String>> callback) {
				provider.entries(stringKeyValueBeanMap -> {
					Map<String, String> stringMap = new HashMap<>();
					if (stringKeyValueBeanMap == null) {
						callback.provide(null);
						return;
					}
					for (Map.Entry<String, KeyValueBean> entry : stringKeyValueBeanMap.entrySet()) {
						stringMap.put(entry.getKey(), entry.getValue().getValue());
					}
					callback.provide(stringMap);
				});
			}

			@Override
			public void size(@Nonnull DataCallback<Integer> callback) {
				provider.size(callback);
			}
		};
	}

}
