/*
 * Copyright 2011-2016 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.springframework.data.domain.Sort.Direction.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexField;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.util.Assert;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;

/**
 * Default implementation of {@link IndexOperations}.
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Komi Innocent
 * @author Christoph Strobl
 */
public class DefaultIndexOperations implements IndexOperations {

	private static final Double ONE = Double.valueOf(1);
	private static final Double MINUS_ONE = Double.valueOf(-1);
	private static final Collection<String> TWO_D_IDENTIFIERS = Arrays.asList("2d", "2dsphere");

	private final MongoOperations mongoOperations;
	private final String collectionName;

	/**
	 * Creates a new {@link DefaultIndexOperations}.
	 * 
	 * @param mongoOperations must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 */
	public DefaultIndexOperations(MongoOperations mongoOperations, String collectionName) {

		Assert.notNull(mongoOperations, "MongoOperations must not be null!");
		Assert.notNull(collectionName, "Collection name can not be null!");

		this.mongoOperations = mongoOperations;
		this.collectionName = collectionName;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.IndexOperations#ensureIndex(org.springframework.data.mongodb.core.index.IndexDefinition)
	 */
	public void ensureIndex(final IndexDefinition indexDefinition) {
		mongoOperations.execute(collectionName, new CollectionCallback<Object>() {
			public Object doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {

				Document indexOptions = indexDefinition.getIndexOptions();

				if (indexOptions != null) {

					IndexOptions ops = new IndexOptions();
					if (indexOptions.containsKey("name")) {
						ops = ops.name(indexOptions.get("name").toString());
					}
					if (indexOptions.containsKey("unique")) {
						ops = ops.unique((Boolean) indexOptions.get("unique"));
					}
					if (indexOptions.containsKey("sparse")) {
						ops = ops.sparse((Boolean) indexOptions.get("sparse"));
					}
					if (indexOptions.containsKey("background")) {
						ops = ops.background((Boolean) indexOptions.get("background"));
					}
					if (indexOptions.containsKey("expireAfterSeconds")) {
						ops = ops.expireAfter((Long) indexOptions.get("expireAfterSeconds"), TimeUnit.SECONDS);
					}
					if (indexOptions.containsKey("min")) {
						ops = ops.min(((Number) indexOptions.get("min")).doubleValue());
					}
					if (indexOptions.containsKey("max")) {
						ops = ops.max(((Number) indexOptions.get("max")).doubleValue());
					}
					if (indexOptions.containsKey("bits")) {
						ops = ops.bits((Integer) indexOptions.get("bits"));
					}
					if (indexOptions.containsKey("bucketSize")) {
						ops = ops.bucketSize(((Number) indexOptions.get("bucketSize")).doubleValue());
					}
					if (indexOptions.containsKey("default_language")) {
						ops = ops.defaultLanguage(indexOptions.get("default_language").toString());
					}
					if (indexOptions.containsKey("language_override")) {
						ops = ops.languageOverride(indexOptions.get("language_override").toString());
					}
					if (indexOptions.containsKey("weights")) {
						ops = ops.weights((Document) indexOptions.get("weights"));
					}

					collection.createIndex(indexDefinition.getIndexKeys(), ops);
				} else {
					collection.createIndex(indexDefinition.getIndexKeys());
				}
				return null;
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.IndexOperations#dropIndex(java.lang.String)
	 */
	public void dropIndex(final String name) {
		mongoOperations.execute(collectionName, new CollectionCallback<Void>() {
			public Void doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {
				collection.dropIndex(name);
				return null;
			}
		});

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.IndexOperations#dropAllIndexes()
	 */
	public void dropAllIndexes() {
		dropIndex("*");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.IndexOperations#getIndexInfo()
	 */
	public List<IndexInfo> getIndexInfo() {

		return mongoOperations.execute(collectionName, new CollectionCallback<List<IndexInfo>>() {
			public List<IndexInfo> doInCollection(MongoCollection<Document> collection)
					throws MongoException, DataAccessException {

				MongoCursor<Document> cursor = collection.listIndexes(Document.class).iterator();
				return getIndexData(cursor);
			}

			private List<IndexInfo> getIndexData(MongoCursor<Document> cursor) {

				List<IndexInfo> indexInfoList = new ArrayList<IndexInfo>();

				while (cursor.hasNext()) {

					Document ix = cursor.next();
					Document keyDocument = (Document) ix.get("key");
					int numberOfElements = keyDocument.keySet().size();

					List<IndexField> indexFields = new ArrayList<IndexField>(numberOfElements);

					for (String key : keyDocument.keySet()) {

						Object value = keyDocument.get(key);

						if (TWO_D_IDENTIFIERS.contains(value)) {
							indexFields.add(IndexField.geo(key));
						} else if ("text".equals(value)) {

							Document weights = (Document) ix.get("weights");
							for (String fieldName : weights.keySet()) {
								indexFields.add(IndexField.text(fieldName, Float.valueOf(weights.get(fieldName).toString())));
							}

						} else {

							Double keyValue = new Double(value.toString());

							if (ONE.equals(keyValue)) {
								indexFields.add(IndexField.create(key, ASC));
							} else if (MINUS_ONE.equals(keyValue)) {
								indexFields.add(IndexField.create(key, DESC));
							}
						}
					}

					String name = ix.get("name").toString();

					boolean unique = ix.containsKey("unique") ? (Boolean) ix.get("unique") : false;
					boolean dropDuplicates = ix.containsKey("dropDups") ? (Boolean) ix.get("dropDups") : false;
					boolean sparse = ix.containsKey("sparse") ? (Boolean) ix.get("sparse") : false;
					String language = ix.containsKey("default_language") ? (String) ix.get("default_language") : "";
					indexInfoList.add(new IndexInfo(indexFields, name, unique, dropDuplicates, sparse, language));
				}

				return indexInfoList;
			}
		});
	}
}
