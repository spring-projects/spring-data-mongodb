/*
 * Copyright 2022 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.util.ObjectUtils;

/**
 * {@link AggregationOperation} implementation that c
 * 
 * @author Christoph Strobl
 * @since 4.0
 */
class BasicAggregationOperation implements AggregationOperation {

	private final Object value;

	BasicAggregationOperation(Object value) {
		this.value = value;
	}

	@Override
	public Document toDocument(AggregationOperationContext context) {

		if (value instanceof Document document) {
			return document;
		}

		if (value instanceof Bson bson) {
			return BsonUtils.asDocument(bson, context.getCodecRegistry());
		}

		if (value instanceof Map map) {
			return new Document(map);
		}

		if (value instanceof String json && BsonUtils.isJsonDocument(json)) {
			return BsonUtils.parse(json, context);
		}

		throw new IllegalStateException(
				String.format("%s cannot be converted to org.bson.Document.", ObjectUtils.nullSafeClassName(value)));
	}
}
