/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.mongodb.util.aggregation;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.data.mongodb.core.aggregation.Field;
import org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 */
public class TestAggregationContext implements AggregationOperationContext {

	private final AggregationOperationContext delegate;

	private TestAggregationContext(AggregationOperationContext delegate) {
		this.delegate = delegate;
	}

	public static AggregationOperationContext contextFor(@Nullable Class<?> type) {

		MappingMongoConverter mongoConverter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE,
				new MongoMappingContext());
		mongoConverter.afterPropertiesSet();

		return contextFor(type, mongoConverter);
	}

	public static AggregationOperationContext contextFor(@Nullable Class<?> type, MongoConverter mongoConverter) {

		if (type == null) {
			return Aggregation.DEFAULT_CONTEXT;
		}

		return new TestAggregationContext(new TypeBasedAggregationOperationContext(type, mongoConverter.getMappingContext(),
				new QueryMapper(mongoConverter)).continueOnMissingFieldReference());
	}

	@Override
	public Document getMappedObject(Document document, @Nullable Class<?> type) {
		return delegate.getMappedObject(document, type);
	}

	@Override
	public FieldReference getReference(Field field) {
		return delegate.getReference(field);
	}

	@Override
	public FieldReference getReference(String name) {
		return delegate.getReference(name);
	}

	@Override
	public CodecRegistry getCodecRegistry() {
		return delegate.getCodecRegistry();
	}
}
