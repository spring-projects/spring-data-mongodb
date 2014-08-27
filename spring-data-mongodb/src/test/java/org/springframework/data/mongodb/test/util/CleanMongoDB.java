/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.test.util;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import com.mongodb.DB;
import com.mongodb.MongoClient;

/**
 * @author Christoph Strobl
 */
public class CleanMongoDB implements TestRule {

	private static final Logger LOGGER = LoggerFactory.getLogger(CleanMongoDB.class);

	public enum Types {
		DATABASE, COLLECTION, INDEX;
	}

	private Set<String> preserveDatabases = new HashSet<String>() {

		private static final long serialVersionUID = -8698807376808700046L;

		{
			add("admin");
			add("local");
		}
	};

	private Set<String> dbNames = new HashSet<String>();
	private Set<String> collectionNames = new HashSet<String>();
	private Set<Types> types = new HashSet<CleanMongoDB.Types>();
	private MongoClient client;

	public CleanMongoDB() {
		this(null);
	}

	public CleanMongoDB(String host, int port) throws UnknownHostException {
		this(new MongoClient(host, port));
	}

	public CleanMongoDB(MongoClient client) {
		this.client = client;
	}

	public static CleanMongoDB everything() {

		CleanMongoDB cleanMongoDB = new CleanMongoDB();
		cleanMongoDB.clean(Types.DATABASE);
		return cleanMongoDB;
	}

	public static CleanMongoDB databases(String... dbNames) {

		CleanMongoDB cleanMongoDB = new CleanMongoDB();
		cleanMongoDB.clean(Types.DATABASE);
		cleanMongoDB.collectionNames.addAll(Arrays.asList(dbNames));
		return cleanMongoDB;
	}

	public static CleanMongoDB indexes() {

		CleanMongoDB cleanMongoDB = new CleanMongoDB();
		cleanMongoDB.clean(Types.INDEX);
		return cleanMongoDB;
	}

	public CleanMongoDB clean(Types... types) {

		this.types.addAll(Arrays.asList(types));
		return this;
	}

	public Statement apply() {
		return apply(null, null);
	}

	public Statement apply(Statement base, Description description) {
		return new MongoCleanStatement(base);
	}

	private class MongoCleanStatement extends Statement {

		private final Statement base;

		public MongoCleanStatement(Statement base) {
			this.base = base;
		}

		@Override
		public void evaluate() throws Throwable {

			if (base != null) {
				base.evaluate();
			}

			boolean isInternal = false;
			if (client == null) {
				client = new MongoClient();
				isInternal = true;
			}

			Collection<String> dbNamesToUse = dbNames;
			if (dbNamesToUse.isEmpty()) {
				dbNamesToUse = client.getDatabaseNames();
			}

			for (String dbName : dbNamesToUse) {

				if (preserveDatabases.contains(dbName.toLowerCase())) {
					continue;
				}

				if (types.contains(Types.DATABASE)) {
					client.dropDatabase(dbName);
					LOGGER.debug("Dropping DB '{}'. ", dbName);
				}

				if (types.contains(Types.COLLECTION)) {

					DB db = client.getDB(dbName);
					Collection<String> collectionsToUse = initCollectionNames(db);
					for (String collectionName : collectionsToUse) {
						if (db.collectionExists(collectionName)) {
							db.getCollectionFromString(collectionName).drop();
							LOGGER.debug("Dropping collection '{}' for DB '{}'. ", collectionName, dbName);
						}
					}
				}

				if (types.contains(Types.INDEX)) {

					DB db = client.getDB(dbName);
					Collection<String> collectionsToUse = initCollectionNames(db);
					for (String collectionName : collectionsToUse) {
						if (db.collectionExists(collectionName)) {
							db.getCollectionFromString(collectionName).dropIndexes();
							LOGGER.debug("Dropping indexes in collection '{}' for DB '{}'. ", collectionName, dbName);
						}
					}
				}
			}

			if (isInternal) {
				client.close();
				client = null;
			}
		}

		private Collection<String> initCollectionNames(DB db) {

			Collection<String> collectionsToUse = collectionNames;
			if (CollectionUtils.isEmpty(collectionsToUse)) {
				collectionsToUse = db.getCollectionNames();
			}
			return collectionsToUse;
		}
	}

}
