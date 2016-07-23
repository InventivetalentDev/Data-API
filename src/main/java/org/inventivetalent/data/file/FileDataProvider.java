package org.inventivetalent.data.file;

import org.inventivetalent.data.DataProvider;
import org.inventivetalent.data.async.AbstractAsyncDataProvider;
import org.inventivetalent.data.async.AsyncDataProvider;
import org.inventivetalent.data.async.DataCallable;
import org.inventivetalent.data.async.DataCallback;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

public class FileDataProvider extends AbstractAsyncDataProvider<String> implements AsyncDataProvider<String>, DataProvider<String> {

	private final File dir;

	public FileDataProvider(File dir) {
		this.dir = dir;
	}

	public FileDataProvider(Executor executor, File dir) {
		super(executor);
		this.dir = dir;
	}

	File getFile(String name) {
		File file = new File(this.dir, name);
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return file;
	}

	void writeFile(File file, String content) {
		try (Writer writer = new FileWriter(file)) {
			writer.write(content);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	String readFile(File file) {
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			StringBuilder builder = new StringBuilder();
			String line;
			while ((line = reader.readLine()) != null) {
				builder.append(line);
			}
			return builder.toString();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void put(@Nonnull String key, @Nonnull String value) {
		File file = getFile(key);
		execute(() -> writeFile(file, value));
	}

	@Override
	public void put(@Nonnull String key, @Nonnull DataCallable<String> valueCallable) {
		File file = getFile(key);
		execute(() -> writeFile(file, valueCallable.provide()));
	}

	@Override
	public void putAll(@Nonnull Map<String, String> map) {
		execute(() -> {
			for (Map.Entry<String, String> entry : map.entrySet()) {
				File file = getFile(entry.getKey());
				writeFile(file, entry.getValue());
			}
		});
	}

	@Nullable
	@Override
	public String get(@Nonnull String key) {
		return readFile(getFile(key));
	}

	@Override
	public boolean contains(@Nonnull String key) {
		return new File(this.dir, key).exists();
	}

	@Override
	public void putAll(@Nonnull DataCallable<Map<String, String>> mapCallable) {
		execute(() -> {
			Map<String, String> map = mapCallable.provide();
			for (Map.Entry<String, String> entry : map.entrySet()) {
				File file = getFile(entry.getKey());
				writeFile(file, entry.getValue());
			}
		});
	}

	@Override
	public void get(@Nonnull String key, @Nonnull DataCallback<String> callback) {
		File file = getFile(key);
		execute(() -> callback.provide(readFile(file)));
	}

	@Override
	public void contains(@Nonnull String key, @Nonnull DataCallback<Boolean> callback) {
		execute(() -> callback.provide(new File(this.dir, key).exists()));
	}

	@Override
	public void remove(@Nonnull String key, @Nonnull DataCallback<String> callback) {
		execute(() -> {
			File file = new File(this.dir, key);
			if (file.exists()) {
				callback.provide(readFile(file));
				file.delete();
			} else {
				callback.provide(null);
			}
		});
	}

	@Override
	public void remove(@Nonnull String key) {
		execute(() -> {
			File file = new File(this.dir, key);
			if (file.exists()) { file.delete(); }
		});
	}

	@Nullable
	@Override
	public String getAndRemove(@Nonnull String key) {
		File file = new File(this.dir, key);
		if (file.exists()) {
			String content = readFile(file);
			file.delete();
			return content;
		}
		return null;
	}

	@Nonnull
	@Override
	public Collection<String> keys() {
		return Arrays.asList(this.dir.list());
	}

	@Nonnull
	@Override
	public Map<String, String> entries() {
		Map<String, String> map = new HashMap<>();
		File[] files = this.dir.listFiles();
		if (files != null) {
			for (File file : files) {
				map.put(file.getName(), readFile(file));
			}
		}
		return map;
	}

	@Override
	public int size() {
		return this.dir.list().length;
	}

	@Override
	public void keys(@Nonnull DataCallback<Collection<String>> callback) {
		execute(() -> callback.provide(Arrays.asList(this.dir.list())));
	}

	@Override
	public void entries(@Nonnull DataCallback<Map<String, String>> callback) {
		execute(() -> {
			Map<String, String> map = new HashMap<>();
			File[] files = this.dir.listFiles();
			if (files != null) {
				for (File file : files) {
					map.put(file.getName(), readFile(file));
				}
			}
			callback.provide(map);
		});
	}

	@Override
	public void size(@Nonnull DataCallback<Integer> callback) {
		execute(() -> callback.provide(this.dir.list().length));
	}
}
