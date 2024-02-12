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
import org.springframework.lang.Nullable;

/**
 * Internal API to trigger the resolution of properties.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
interface ValueResolver {

	/**
	 * Resolves the value for the given {@link MongoPersistentProperty} within the given {@link Document} using the given
	 * {@link ValueExpressionEvaluator} and {@link ObjectPath}.
	 *
	 * @param prop
	 * @param bson
	 * @param evaluator
	 * @param path
	 * @return
	 */
	@Nullable
	Object getValueInternal(MongoPersistentProperty prop, Bson bson, ValueExpressionEvaluator evaluator, ObjectPath path);
}
