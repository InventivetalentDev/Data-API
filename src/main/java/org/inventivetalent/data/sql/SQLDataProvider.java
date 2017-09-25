package org.inventivetalent.data.sql;

import lombok.NonNull;
import org.inventivetalent.data.async.AbstractAsyncDataProvider;
import org.inventivetalent.data.async.DataCallable;
import org.inventivetalent.data.async.DataCallback;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

@SuppressWarnings({"unused", "WeakerAccess"})
public class SQLDataProvider extends AbstractAsyncDataProvider<String> {

	static final String CREATE_TABLE_IF_NOT_EXISTS = "CREATE TABLE IF NOT EXISTS `%s` "//
			+ "("//
			+ "`_Key` VARCHAR(255) NOT NULL UNIQUE, "//
			+ "`_Value` TEXT"//
			+ ");";//
	static final String SELECT_VALUE_WHERE_KEY = "SELECT * FROM %1$s WHERE _Key=? LIMIT 1;";
	static final String SELECT_VALUES_IN = "SELECT _Key, _Value FROM %1s WHERE _Key in (?);";
	static final String INSERT_OR_UPDATE = "INSERT INTO %1$s (_Key,_Value) VALUES (?,?) ON DUPLICATE KEY UPDATE _Value=VALUES(_Value);";
	static final String INSERT_MULTIPLE_OR_UPDATE = "INSERT INTO %1$s (_Key,_Value) VALUES%2$s ON DUPLICATE KEY UPDATE _Value=VALUES(_Value);";
	static final String DELETE_WHERE_KEY = "DELETE FROM %1$s WHERE _Key=?;";
	static final String COUNT_WHERE_KEY = "SELECT count(*) FROM %1$s WHERE _Key=?;";
	static final String SELECT_KEYS = "SELECT _Key FROM %1$s;";
	static final String SELECT_ENTRIES = "SELECT _Key, _Value FROM %1$s;";
	static final String COUNT = "SELECT COUNT(*) AS count FROM %1$s;";

	private final Connection connection;
	private final String table;

	public SQLDataProvider(Connection connection, String table) {
		this.connection = connection;
		this.table = table;

		createTableIfNotExists();
	}

	public SQLDataProvider(Executor executor, Connection connection, String table) {
		super(executor);
		this.connection = connection;
		this.table = table;

		createTableIfNotExists();
	}

	public SQLDataProvider(String host, String user, String pass, String table) throws SQLException {
		this.connection = DriverManager.getConnection(host, user, pass);
		this.table = table;

		createTableIfNotExists();
	}

	public SQLDataProvider(Executor executor, String host, String user, String pass, String table) throws SQLException {
		super(executor);
		this.connection = DriverManager.getConnection(host, user, pass);
		this.table = table;

		createTableIfNotExists();
	}

	void createTableIfNotExists() {
		execute(() -> {
			try {
				connection.prepareStatement(String.format(CREATE_TABLE_IF_NOT_EXISTS, table)).executeUpdate();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}

	@Override
	public void put(@NonNull String key, @NonNull String value) {
		execute(() -> {
			try {
				PreparedStatement stmt = connection.prepareStatement(String.format(INSERT_OR_UPDATE, table));
				stmt.setString(1, key);
				stmt.setString(2, value);
				stmt.executeUpdate();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}

	@Override
	public void put(@NonNull String key, @NonNull DataCallable<String> valueCallable) {
		execute(() -> {
			try {
				PreparedStatement stmt = connection.prepareStatement(String.format(INSERT_OR_UPDATE, table));
				stmt.setString(1, key);
				stmt.setString(2, valueCallable.provide());
				stmt.executeUpdate();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}

	@Override
	public void putAll(@NonNull Map<String, String> map) {
		execute(() -> {
			StringBuilder arrayString = new StringBuilder();
			boolean first = true;
			for (Map.Entry<String, String> entry : map.entrySet()) {
				if (!first) {
					arrayString.append(", ");
				}
				arrayString.append("(`").append(entry.getKey()).append("`, `").append(entry.getValue()).append("`)");

				first = false;
			}
			try {
				PreparedStatement stmt = connection.prepareStatement(String.format(INSERT_MULTIPLE_OR_UPDATE, table, arrayString.toString()));
				stmt.executeUpdate();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}

	@Override
	public void putAll(@NonNull DataCallable<Map<String, String>> mapCallable) {
		execute(() -> {
			Map<String, String> map = mapCallable.provide();
			StringBuilder arrayString = new StringBuilder();
			boolean first = true;
			for (Map.Entry<String, String> entry : map.entrySet()) {
				if (!first) {
					arrayString.append(", ");
				}
				arrayString.append("(`").append(entry.getKey()).append("`, `").append(entry.getValue()).append("`)");

				first = false;
			}
			try {
				PreparedStatement stmt = connection.prepareStatement(String.format(INSERT_MULTIPLE_OR_UPDATE, table, arrayString.toString()));
				stmt.executeUpdate();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}

	@Override
	public void get(@NonNull String key, @NonNull DataCallback<String> callback) {
		execute(() -> {
			try {
				PreparedStatement stmt = connection.prepareStatement(String.format(SELECT_VALUE_WHERE_KEY, table));
				stmt.setString(1, key);
				ResultSet resultSet = stmt.executeQuery();
				if (resultSet.next()) {
					callback.provide(resultSet.getString("_Value"));
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}

	@Override
	public void contains(@NonNull String key, @NonNull DataCallback<Boolean> callback) {
		execute(() -> {
			try {
				PreparedStatement preparedStatement = connection.prepareStatement(String.format(COUNT_WHERE_KEY, table));
				preparedStatement.setString(1, key);
				callback.provide(preparedStatement.executeQuery().next());
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}

	@Override
	public void remove(@NonNull String key, @NonNull DataCallback<String> callback) {
		get(key, callback);
		remove(key);
	}

	@Override
	public void remove(@NonNull String key) {
		execute(() -> {
			try {
				PreparedStatement preparedStatement = connection.prepareStatement(String.format(DELETE_WHERE_KEY, table));
				preparedStatement.setString(1, key);
				preparedStatement.executeUpdate();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}

	@Override
	public void keys(@NonNull DataCallback<Collection<String>> callback) {
		execute(() -> {
			try {
				Set<String> keys = new HashSet<>();
				ResultSet resultSet = connection.prepareStatement(String.format(SELECT_KEYS, table)).executeQuery();
				while (resultSet.next()) {
					keys.add(resultSet.getString("_Key"));
				}
				callback.provide(keys);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}

	@Override
	public void entries(@NonNull DataCallback<Map<String, String>> callback) {
		execute(() -> {
			try {
				Map<String, String> entries = new HashMap<>();
				ResultSet resultSet = connection.prepareStatement(String.format(SELECT_ENTRIES, table)).executeQuery();
				while (resultSet.next()) {
					entries.put(resultSet.getString("_Key"), resultSet.getString("_Value"));
				}
				callback.provide(entries);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}

	@Override
	public void size(@NonNull DataCallback<Integer> callback) {
		execute(() -> {
			try {
				PreparedStatement stmt = connection.prepareStatement(String.format(COUNT, table));
				ResultSet resultSet = stmt.executeQuery();
				if (resultSet.next()) {
					callback.provide(resultSet.getInt("count"));
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}
}
