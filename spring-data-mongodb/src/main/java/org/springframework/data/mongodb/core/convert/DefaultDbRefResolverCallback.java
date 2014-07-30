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
package org.springframework.data.mongodb.core.convert;

import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

import com.mongodb.DBObject;

/**
 * @author Oliver Gierke
 */
class DefaultDbRefResolverCallback implements DbRefResolverCallback {

	private final DBObject surroundingObject;
	private final Object parent;
	private final ValueResolver resolver;
	private final SpELExpressionEvaluator evaluator;

	/**
	 * @param surroundingObject must not be {@literal null}.
	 * @param parent must not be {@literal null}.
	 * @param resolver must not be {@literal null}.
	 * @param evaluator must not be {@literal null}.
	 */
	public DefaultDbRefResolverCallback(DBObject surroundingObject, Object parent, ValueResolver resolver,
			SpELExpressionEvaluator evaluator) {

		this.surroundingObject = surroundingObject;
		this.parent = parent;
		this.resolver = resolver;
		this.evaluator = evaluator;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.DbRefResolverCallback#resolve(org.springframework.data.mongodb.core.mapping.MongoPersistentProperty)
	 */
	@Override
	public Object resolve(MongoPersistentProperty property) {
		return resolver.getValueInternal(property, surroundingObject, evaluator, parent);
	}
}
