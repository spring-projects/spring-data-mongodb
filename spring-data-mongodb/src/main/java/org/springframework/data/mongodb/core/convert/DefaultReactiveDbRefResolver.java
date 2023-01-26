/*
 * Copyright 2023 the original author or authors.
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

import reactor.core.publisher.Mono;

import java.util.List;

import org.bson.Document;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

import com.mongodb.DBRef;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * @author Christoph Strobl
 * @since 4.1
 */
public class DefaultReactiveDbRefResolver implements ReactiveDbRefResolver {

	ReactiveMongoDatabaseFactory dbFactory;

	public DefaultReactiveDbRefResolver(ReactiveMongoDatabaseFactory dbFactory) {
		this.dbFactory = dbFactory;
	}

	@Nullable
	@Override
	public Mono<Object> resolveDbRef(MongoPersistentProperty property, @Nullable DBRef dbref,
			DbRefResolverCallback callback, DbRefProxyHandler proxyHandler) {
		return null;
	}

	@Nullable
	@Override
	public Document fetch(DBRef dbRef) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<Document> bulkFetch(List<DBRef> dbRefs) {
		throw new UnsupportedOperationException();
	}

	@Nullable
	@Override
	public Mono<Document> initFetch(DBRef dbRef) {

		Mono<MongoDatabase> mongoDatabase = StringUtils.hasText(dbRef.getDatabaseName())
				? dbFactory.getMongoDatabase(dbRef.getDatabaseName())
				: dbFactory.getMongoDatabase();
		return mongoDatabase
				.flatMap(db -> Mono.from(db.getCollection(dbRef.getCollectionName()).find(new Document("_id", dbRef.getId()))));
	}

	@Nullable
	@Override
	public Mono<Object> resolveReference(MongoPersistentProperty property, Object source,
			ReferenceLookupDelegate referenceLookupDelegate, MongoEntityReader entityReader) {
		if (source instanceof DBRef dbRef) {

		}
		throw new UnsupportedOperationException();
	}
}
