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

import java.util.Collections;
import java.util.List;

import com.mongodb.MongoClient;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 * @since 2016/12
 */
public class MongoTemplateBuilder {

	private static MongoTemplate template;

	static {
		template = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "test"));
	}


	public static SourceCollectionBuilder find(Query query) {
		return new Builder(query);
	}

	public interface SourceCollectionBuilder {
		MappingRootTypeBuilder in(String collection);
		MappingDestinationTypeBuilder inCollectionOf(Class<?> sourceType);
	}

	public interface ExecuteBuilder {
		<T> List<T> execute();
	}

	public interface MappingRootTypeBuilder {
		MappingDestinationTypeBuilder of(Class<?> sourceType);
	}

	public interface MappingDestinationTypeBuilder extends ExecuteBuilder, UpdateBuilder {
		UpdateBuilder mappingTo(Class<?> targetType);
	}

	public interface UpdateBuilder extends ExecuteBuilder {
		ExecuteUpdateBuilder andApply(Update update);
	}

	public interface  ExecuteUpdateBuilder {
		void execute();
	}

	private static class Builder implements ExecuteBuilder, MappingRootTypeBuilder, MappingDestinationTypeBuilder, SourceCollectionBuilder, UpdateBuilder {

		private Query query;
		private String collectionName;
		private Update update;
		private Class<?> sourceType;
		private Class<?> targetType;

		 Builder(Query query) {
			this.query = query;
		}

		@Override
		public <T> List<T> execute() {

			Document queryObject = query != null ? query.getQueryObject() : new Document();
			Document fieldsObject = query != null ? query.getFieldsObject() : new Document();

			Class<?> typeToUse = targetType != null ? targetType  :sourceType;

			if(update == null) {
				return (List<T>) template.doFind(collectionName, queryObject, fieldsObject, sourceType, typeToUse,null );
			}

			return Collections.emptyList();
		}

		@Override
		public MappingDestinationTypeBuilder of(Class<?> sourceType) {


			setSourceType(sourceType);
			return this;
		}

		private void setSourceType(Class<?> sourceType) {

		 	if(!StringUtils.hasText(collectionName)) {
		 		this.collectionName = template.determineCollectionName(sourceType);
			}

			this.sourceType = sourceType;
		}


		@Override
		public UpdateBuilder mappingTo(Class<?> targetType) {

			this.targetType = targetType;
			return this;
		}

		@Override
		public MappingRootTypeBuilder in(String collection) {

			this.collectionName = collection;
			return this;
		}

		@Override
		public MappingDestinationTypeBuilder inCollectionOf(Class<?> sourceType) {

			setSourceType(sourceType);
			return this;
		}

		@Override
		public ExecuteUpdateBuilder andApply(Update update) {
			this.update = update;
			return () -> {

				Class<?> typeToUse = targetType != null ? targetType  :sourceType;
				template.doUpdate(collectionName, query, update, typeToUse, true, true);
			};
		}
	}

}
