/*
 * Copyright 2014-2024 the original author or authors.
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

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.mapping.model.ValueExpressionEvaluator;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

/**
 * Default implementation of {@link DbRefResolverCallback}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class DefaultDbRefResolverCallback implements DbRefResolverCallback {

	private final Bson surroundingObject;
	private final ObjectPath path;
	private final ValueResolver resolver;
	private final ValueExpressionEvaluator evaluator;

	/**
	 * Creates a new {@link DefaultDbRefResolverCallback} using the given {@link Document}, {@link ObjectPath},
	 * {@link ValueResolver} and {@link ValueExpressionEvaluator}.
	 *
	 * @param surroundingObject must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param evaluator must not be {@literal null}.
	 * @param resolver must not be {@literal null}.
	 */
	DefaultDbRefResolverCallback(Bson surroundingObject, ObjectPath path, ValueExpressionEvaluator evaluator,
			ValueResolver resolver) {

		this.surroundingObject = surroundingObject;
		this.path = path;
		this.resolver = resolver;
		this.evaluator = evaluator;
	}

	@Override
	public Object resolve(MongoPersistentProperty property) {
		return resolver.getValueInternal(property, surroundingObject, evaluator, path);
	}
}
