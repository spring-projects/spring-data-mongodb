package org.springframework.data.mongodb.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.mongodb.DB;
import org.springframework.transaction.support.ResourceHolderSupport;
import org.springframework.util.Assert;

class DbHolder extends ResourceHolderSupport {
	private static final Object DEFAULT_KEY = new Object();

	private final Map<Object, DB> dbMap = new ConcurrentHashMap<Object, DB>();

	public DbHolder(DB db) {
		addDB(db);
	}

	public DbHolder(Object key, DB db) {
		addDB(key, db);
	}

	public DB getDB() {
		return getDB(DEFAULT_KEY);
	}

	public DB getDB(Object key) {
		return this.dbMap.get(key);
	}

	public DB getAnyDB() {
		if (!this.dbMap.isEmpty()) {
			return this.dbMap.values().iterator().next();
		}
		return null;
	}

	public void addDB(DB session) {
		addDB(DEFAULT_KEY, session);
	}

	public void addDB(Object key, DB session) {
		Assert.notNull(key, "Key must not be null");
		Assert.notNull(session, "DB must not be null");
		this.dbMap.put(key, session);
	}

	public DB removeDB(Object key) {
		return this.dbMap.remove(key);
	}

	public boolean containsDB(DB session) {
		return this.dbMap.containsValue(session);
	}

	public boolean isEmpty() {
		return this.dbMap.isEmpty();
	}

	public boolean doesNotHoldNonDefaultDB() {
		synchronized (this.dbMap) {
			return this.dbMap.isEmpty() || (this.dbMap.size() == 1 && this.dbMap.containsKey(DEFAULT_KEY));
		}
	}

}
