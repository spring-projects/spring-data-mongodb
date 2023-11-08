/*
 * Copyright 2013-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.convert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoDatabaseUtils;
import org.springframework.data.mongodb.core.convert.ReferenceLoader.DocumentReferenceQuery;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.FieldName;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.DBRef;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

/**
 * A {@link DbRefResolver} that resolves {@link org.springframework.data.mongodb.core.mapping.DBRef}s by delegating to a
 * {@link DbRefResolverCallback} than is able to generate lazy loading proxies.
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.4
 */
public class DefaultDbRefResolver extends DefaultReferenceResolver implements DbRefResolver, ReferenceResolver {

	private static final Log LOGGER = LogFactory.getLog(DefaultDbRefResolver.class);

	private final MongoDatabaseFactory mongoDbFactory;

	/**
	 * Creates a new {@link DefaultDbRefResolver} with the given {@link MongoDatabaseFactory}.
	 *
	 * @param mongoDbFactory must not be {@literal null}.
	 */
	public DefaultDbRefResolver(MongoDatabaseFactory mongoDbFactory) {

		super(new MongoDatabaseFactoryReferenceLoader(mongoDbFactory), mongoDbFactory.getExceptionTranslator());

		Assert.notNull(mongoDbFactory, "MongoDbFactory translator must not be null");

		this.mongoDbFactory = mongoDbFactory;
	}

	@Override
	public Object resolveDbRef(MongoPersistentProperty property, @Nullable DBRef dbref, DbRefResolverCallback callback,
			DbRefProxyHandler handler) {

		Assert.notNull(property, "Property must not be null");
		Assert.notNull(callback, "Callback must not be null");
		Assert.notNull(handler, "Handler must not be null");

		if (isLazyDbRef(property)) {
			return createLazyLoadingProxy(property, dbref, callback, handler);
		}

		return callback.resolve(property);
	}

	@Override
	public Document fetch(DBRef dbRef) {
		return getReferenceLoader().fetchOne(
				DocumentReferenceQuery.forSingleDocument(Filters.eq(FieldName.ID.name(), dbRef.getId())),
				ReferenceCollection.fromDBRef(dbRef));
	}

	@Override
	public List<Document> bulkFetch(List<DBRef> refs) {

		Assert.notNull(mongoDbFactory, "Factory must not be null");
		Assert.notNull(refs, "DBRef to fetch must not be null");

		if (refs.isEmpty()) {
			return Collections.emptyList();
		}

		String collection = refs.iterator().next().getCollectionName();
		List<Object> ids = new ArrayList<>(refs.size());

		for (DBRef ref : refs) {

			if (!collection.equals(ref.getCollectionName())) {
				throw new InvalidDataAccessApiUsageException(
						"DBRefs must all target the same collection for bulk fetch operation");
			}

			ids.add(ref.getId());
		}

		DBRef databaseSource = refs.iterator().next();
		MongoCollection<Document> mongoCollection = getCollection(databaseSource);

		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace(String.format("Bulk fetching DBRefs %s from %s.%s", ids,
					StringUtils.hasText(databaseSource.getDatabaseName()) ? databaseSource.getDatabaseName()
							: mongoCollection.getNamespace().getDatabaseName(),
					databaseSource.getCollectionName()));
		}

		List<Document> result = mongoCollection //
				.find(new Document(BasicMongoPersistentProperty.ID_FIELD_NAME, new Document("$in", ids))) //
				.into(new ArrayList<>(ids.size()));

		return ids.stream() //
				.flatMap(id -> documentWithId(id, result)) //
				.collect(Collectors.toList());
	}

	/**
	 * Creates a proxy for the given {@link MongoPersistentProperty} using the given {@link DbRefResolverCallback} to
	 * eventually resolve the value of the property.
	 *
	 * @param property must not be {@literal null}.
	 * @param dbref can be {@literal null}.
	 * @param callback must not be {@literal null}.
	 * @return
	 */
	private Object createLazyLoadingProxy(MongoPersistentProperty property, @Nullable DBRef dbref,
			DbRefResolverCallback callback, DbRefProxyHandler handler) {

		Object lazyLoadingProxy = getProxyFactory().createLazyLoadingProxy(property, callback, dbref);

		return handler.populateId(property, dbref, lazyLoadingProxy);
	}

	/**
	 * Returns whether the property shall be resolved lazily.
	 *
	 * @param property must not be {@literal null}.
	 * @return
	 */
	private boolean isLazyDbRef(MongoPersistentProperty property) {
		return property.getDBRef() != null && property.getDBRef().lazy();
	}

	/**
	 * Returns document with the given identifier from the given list of {@link Document}s.
	 *
	 * @param identifier
	 * @param documents
	 * @return
	 */
	private static Stream<Document> documentWithId(Object identifier, Collection<Document> documents) {

		return documents.stream() //
				.filter(it -> it.get(BasicMongoPersistentProperty.ID_FIELD_NAME).equals(identifier)) //
				.limit(1);
	}

	/**
	 * Customization hook for obtaining the {@link MongoCollection} for a given {@link DBRef}.
	 *
	 * @param dbref must not be {@literal null}.
	 * @return the {@link MongoCollection} the given {@link DBRef} points to.
	 * @since 2.1
	 */
	protected MongoCollection<Document> getCollection(DBRef dbref) {

		return MongoDatabaseUtils.getDatabase(dbref.getDatabaseName(), mongoDbFactory)
				.getCollection(dbref.getCollectionName(), Document.class);
	}

	protected MongoCollection<Document> getCollection(ReferenceCollection context) {

		return MongoDatabaseUtils.getDatabase(context.getDatabase(), mongoDbFactory).getCollection(context.getCollection(),
				Document.class);
	}
}
