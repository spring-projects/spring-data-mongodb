/*
 * Copyright 2016. the original author or authors.
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

import java.util.List;

import org.bson.Document;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.util.StringUtils;

import com.mongodb.MongoClient;

/**
 * @author Christoph Strobl
 * @since 2016/12
 */
public class MongoTemplateBuilderII {

	private static MongoTemplate template;

	static {
		template = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "test"));
	}

	public static <S> FindExecutionBuilder<S, S> query(Class<S> returnType) {
		return new FindBuilder<S, S>(returnType);
	}

	interface FindExecutionBuilder<S, T> extends FromCollectionBuilder<S, T>, FindMappingToBuilder<S, T> {
	}

	interface FromCollectionBuilder<S, T> extends FindQueryByBuilder<S, T> {
		FindMappingToBuilder in(String collection);
	}

	interface FindMappingToBuilder<S, T> extends FindQueryByBuilder<S, T> {
		<T> FindQueryByBuilder<S, T> returningResultAs(Class<T> resultType);
	}

	interface FindQueryByBuilder<S, T> {
		List<T> findBy(Query query);
		List<T> all();
		List<T> findAnddRemoveBy(Query query);
	}

	static class FindBuilder<S, T> implements FindExecutionBuilder<S, T>, FromCollectionBuilder<S, T>,
			FindMappingToBuilder<S, T>, FindQueryByBuilder<S, T> {

		private Class<?> returnType;
		private Class<?> rootType;
		private String collectionName;
		private Query query;

		public FindBuilder(Class<?> returnType) {
			this.returnType = returnType;
		}

		@Override
		public FindMappingToBuilder in(String collection) {

			this.collectionName = collection;
			return this;
		}

		@Override
		public List<T> findBy(Query query) {

			Document queryObject = query != null ? query.getQueryObject() : new Document();
			Document fieldsObject = query != null ? query.getFieldsObject() : new Document();

			if (rootType == null) {
				rootType = returnType;
			}

			return (List<T>) template.doFind(
					StringUtils.hasText(collectionName) ? collectionName : template.determineCollectionName(rootType),
					queryObject, fieldsObject, rootType, returnType, null);
		}

		@Override
		public <T1> FindQueryByBuilder<S, T1> returningResultAs(Class<T1> resultType) {

			if (this.rootType == null) {
				this.rootType = this.returnType;
			}
			this.returnType = resultType;
			return (FindQueryByBuilder<S, T1>) this;
		}

		@Override
		public List<T> all() {
			return findBy(null);
		}

		@Override
		public List<T> findAnddRemoveBy(Query query) {


			// We need an additional overload for this.
			// return template.findAndRemove(query)

			return null;
		}
	}

	public static UpdateFullBuilder  update(Class<?> type) {
		return new UpdateBuilder(type);
	}

	public interface UpdateFullBuilder extends UpdateUpsertBuilder, MultiUpdateBuilder, UpdateCollectionBuilder, UpdateUpdateBuilder, UpdateQueryBuilder {

	}


	public interface UpdateCollectionBuilder {
		UpdateFullBuilder in(String collection);
	}

	public interface UpdateUpsertBuilder {
		UpdateFullBuilder upsert();
	}

	public interface MultiUpdateBuilder {
		UpdateFullBuilder single();
	}

	public interface UpdateQueryBuilder {
		UpdateFullBuilder where(Query query);
	}

	public interface UpdateUpdateBuilder {
		void apply(Update update);
	}

	static class UpdateBuilder implements UpdateFullBuilder {

		private Class<?> rootType;
		private String collectionName;
		private boolean upsert = false;
		private boolean multi = true;
		private Query query;

		public UpdateBuilder(Class<?> rootType) {
			this.rootType = rootType;
		}

		@Override
		public UpdateFullBuilder in(String collection) {

			this.collectionName = collection;
			return this;
		}

		@Override
		public UpdateFullBuilder upsert() {

			this.upsert = true;
			return this;
		}

		@Override
		public UpdateFullBuilder single() {

			this.multi = false;
			return this;
		}

		@Override
		public UpdateFullBuilder where(Query query) {

			this.query = query;
			return this;
		}

		@Override
		public void apply(Update update) {

			String collection = StringUtils.hasText(collectionName) ? collectionName : template.determineCollectionName(rootType);
			template.doUpdate(collection, query, update, rootType, upsert, multi);
		}
	}

}
