package org.inventivetalent.data.ebean;

import com.avaje.ebean.EbeanServer;
import org.inventivetalent.data.async.AbstractAsyncDataProvider;
import org.inventivetalent.data.async.DataCallable;
import org.inventivetalent.data.async.DataCallback;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

public class EbeanDataProvider<V extends KeyBean> extends AbstractAsyncDataProvider<V> {

	private final EbeanServer database;
	private final Class<V>    beanClass;

	public EbeanDataProvider(EbeanServer database, Class<V> beanClass) {
		this.database = database;
		this.beanClass = beanClass;
	}

	public EbeanDataProvider(Executor executor, EbeanServer database, Class<V> beanClass) {
		super(executor);
		this.database = database;
		this.beanClass = beanClass;
	}

	public EbeanServer getDatabase() {
		return database;
	}

	public V newBean() {
		try {
			return beanClass.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void put(@Nonnull String key, @Nonnull V value) {
		execute(() -> {
			V entry;
			boolean exists = (entry = getDatabase().find(beanClass).where().eq("key", key).findUnique()) != null;

			int id = -1;
			long version = 0;
			if (entry != null) {
				id = entry.getId();
				version = entry.getVersion();
			}

			entry = value;
			entry.setKey(key);
			if (!exists) {
				getDatabase().save(entry);
			} else {
				entry.setId(id);
				entry.setVersion(version);
				getDatabase().update(entry);
			}
		});
	}

	@Override
	public void put(@Nonnull String key, @Nonnull DataCallable<V> valueCallable) {
		execute(() -> {
			V entry;
			boolean exists = (entry = getDatabase().find(beanClass).where().eq("key", key).findUnique()) != null;
			entry = valueCallable.provide();
			entry.setKey(key);
			if (!exists) {
				getDatabase().save(entry);
			} else {
				getDatabase().update(entry);
			}
		});
	}

	@Override
	public void putAll(@Nonnull Map<String, V> map) {
		execute(() -> {
			for (Map.Entry<String, V> mEntry : map.entrySet()) {
				V entry;
				boolean exists = (entry = getDatabase().find(beanClass).where().eq("key", mEntry.getKey()).findUnique()) != null;
				entry = mEntry.getValue();
				entry.setKey(mEntry.getKey());
				if (!exists) {
					getDatabase().save(entry);
				} else {
					getDatabase().update(entry);
				}
			}
		});
	}

	@Override
	public void putAll(@Nonnull DataCallable<Map<String, V>> mapCallable) {
		execute(() -> {
			for (Map.Entry<String, V> mEntry : mapCallable.provide().entrySet()) {
				V entry;
				boolean exists = (entry = getDatabase().find(beanClass).where().eq("key", mEntry.getKey()).findUnique()) != null;
				entry = mEntry.getValue();
				entry.setKey(mEntry.getKey());
				if (!exists) {
					getDatabase().save(entry);
				} else {
					getDatabase().update(entry);
				}
			}
		});
	}

	@Override
	public void get(@Nonnull String key, @Nonnull DataCallback<V> callback) {
		execute(() -> callback.provide(getDatabase().find(beanClass).where().eq("key", key).findUnique()));
	}

	@Override
	public void contains(@Nonnull String key, @Nonnull DataCallback<Boolean> callback) {
		execute(() -> callback.provide(getDatabase().find(beanClass).where().eq("key", key).findRowCount() > 0));
	}

	@Override
	public void remove(@Nonnull String key, @Nonnull DataCallback<V> callback) {
		execute(() -> {
			V value = getDatabase().find(beanClass).where().eq("key", key).findUnique();
			if (value != null) { getDatabase().delete(value); }
			callback.provide(value);
		});
	}

	@Override
	public void remove(@Nonnull String key) {
		execute(() -> {
			V value = getDatabase().find(beanClass).where().eq("key", key).findUnique();
			if (value != null) { getDatabase().delete(value); }
		});
	}

	@Override
	public void keys(@Nonnull DataCallback<Collection<String>> callback) {
		execute(() -> {
			Set<V> entries = getDatabase().find(beanClass).select("key").findSet();
			Set<String> keys = entries.stream().map(V::getKey).collect(Collectors.toSet());
			callback.provide(keys);
		});
	}

	@Override
	public void entries(@Nonnull DataCallback<Map<String, V>> callback) {
		execute(() -> {
			Set<V> entries = getDatabase().find(beanClass).findSet();
			Map<String, V> map = new HashMap<>();
			for (V entry : entries) {
				map.put(entry.getKey(), entry);
			}
			callback.provide(map);
		});
	}

	@Override
	public void size(@Nonnull DataCallback<Integer> callback) {
		execute(() -> callback.provide(getDatabase().find(beanClass).findRowCount()));
	}
}
