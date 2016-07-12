package org.inventivetalent.data.mongodb;

import com.google.gson.JsonObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.inventivetalent.data.async.AbstractAsyncDataProvider;
import org.inventivetalent.data.async.DataCallable;
import org.inventivetalent.data.async.DataCallback;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

public class MongoDbDataProvider extends AbstractAsyncDataProvider<JsonObject> {

	private static final UpdateOptions UPSERT_OPTIONS = new UpdateOptions().upsert(true);

	private final MongoClient               client;
	private final MongoCollection<Document> collection;
	private String keyField = "_id";

	public MongoDbDataProvider(MongoClient client, String database, String collection) {
		this.client = client;
		this.collection = this.client.getDatabase(database).getCollection(collection);
	}

	public MongoDbDataProvider(Executor executor, MongoClient client, String database, String collection) {
		super(executor);
		this.client = client;
		this.collection = this.client.getDatabase(database).getCollection(collection);
	}

	public MongoDbDataProvider(String host, int port, String user, char[] pass, String authDatabase, String database, String collection) {
		MongoCredential credential = user != null && pass != null && authDatabase != null ? MongoCredential.createScramSha1Credential(user, authDatabase, pass) : null;
		this.client = credential != null ? new MongoClient(new ServerAddress(host, port), Collections.singletonList(credential)) : new MongoClient(new ServerAddress(host, port));
		this.collection = this.client.getDatabase(database).getCollection(collection);
	}

	public MongoDbDataProvider(Executor executor, String host, int port, String user, char[] pass, String authDatabase, String database, String collection) {
		super(executor);
		MongoCredential credential = MongoCredential.createScramSha1Credential(user, authDatabase, pass);
		this.client = new MongoClient(new ServerAddress(host, port), Collections.singletonList(credential));
		this.collection = this.client.getDatabase(database).getCollection(collection);
	}

	public MongoDbDataProvider setKeyField(String keyField) {
		this.keyField = keyField;
		return this;
	}

	public MongoCollection<Document> getCollection() {
		return collection;
	}

	@Override
	public void put(@Nonnull String key, @Nonnull JsonObject value) {
		execute(() -> this.collection.updateOne(new Document(this.keyField, key).append(this.keyField, key), new Document("$set", DocumentParser.toDocument(value)), UPSERT_OPTIONS));
	}

	@Override
	public void put(@Nonnull String key, @Nonnull DataCallable<JsonObject> valueCallable) {
		execute(() -> this.collection.updateOne(new Document(this.keyField, key).append(this.keyField, key), new Document("$set", DocumentParser.toDocument(valueCallable.provide())), UPSERT_OPTIONS));
	}

	/**
	 * Note: Only runs {@code insert} instead of {@code update} with upsert.
	 *
	 * @param map values to insert
	 */
	@Override
	public void putAll(@Nonnull Map<String, JsonObject> map) {
		List<Document> documents = map.entrySet().stream().map(entry -> DocumentParser.toDocument(entry.getValue()).append(this.keyField, entry.getKey())).collect(Collectors.toList());
		execute(() -> this.collection.insertMany(documents));
	}

	@Override
	public void putAll(@Nonnull DataCallable<Map<String, JsonObject>> mapCallable) {
		List<Document> documents = mapCallable.provide().entrySet().stream().map(entry -> DocumentParser.toDocument(entry.getValue()).append(this.keyField, entry.getKey())).collect(Collectors.toList());
		execute(() -> this.collection.insertMany(documents));
	}

	@Override
	public void get(@Nonnull String key, @Nonnull DataCallback<JsonObject> callback) {
		execute(() -> callback.provide(DocumentParser.toJson(this.collection.find(new Document(this.keyField, key)).limit(1).first())));
	}

	@Override
	public void contains(@Nonnull String key, @Nonnull DataCallback<Boolean> callback) {
		execute(() -> callback.provide(this.collection.count(new Document(this.keyField, key)) > 0));
	}

	@Override
	public void remove(@Nonnull String key, @Nonnull DataCallback<JsonObject> callback) {
		execute(() -> callback.provide(DocumentParser.toJson(this.collection.findOneAndDelete(new Document(this.keyField, key)))));
	}

	@Override
	public void remove(@Nonnull String key) {
		execute(() -> this.collection.deleteOne(new Document(this.keyField, key)));
	}

	@Override
	public void keys(@Nonnull DataCallback<Collection<String>> callback) {
		this.entries(stringJsonObjectMap -> {
			assert stringJsonObjectMap != null;
			callback.provide(stringJsonObjectMap.keySet());
		});
	}

	@Override
	public void entries(@Nonnull DataCallback<Map<String, JsonObject>> callback) {
		execute(() -> {
			Map<String, JsonObject> map = new HashMap<>();
			for (Document doc : collection.find()) {
				map.put(doc.getString(keyField), DocumentParser.toJson(doc));
			}
			callback.provide(map);
		});
	}

	@Override
	public void size(@Nonnull DataCallback<Integer> callback) {
		execute(() -> callback.provide((int) this.collection.count()));
	}
}
