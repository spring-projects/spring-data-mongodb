/*
 * Copyright 2014-2025 the original author or authors.
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

import java.util.function.Function;

import org.bson.Document;
import org.jspecify.annotations.Nullable;

import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ValueExpressionEvaluator;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.Wrapped;

import com.mongodb.DBRef;

/**
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
record DefaultDbRefProxyHandler(
		MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext, ValueResolver resolver,
		Function<Object, ValueExpressionEvaluator> evaluatorFactory) implements DbRefProxyHandler {

	@Override
	public Object populateId(MongoPersistentProperty property, @Nullable DBRef source, Object proxy) {

		if (source == null) {
			return proxy;
		}

		MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(property);
		MongoPersistentProperty idProperty = entity.getRequiredIdProperty();

		if (idProperty.usePropertyAccess()) {
			return proxy;
		}

		ValueExpressionEvaluator evaluator = evaluatorFactory.apply(proxy);
		PersistentPropertyAccessor<?> accessor = entity.getPropertyAccessor((Wrapped) () -> proxy);

		Document object = new Document(idProperty.getFieldName(), source.getId());
		ObjectPath objectPath = ObjectPath.ROOT.push(proxy, entity, null);
		accessor.setProperty(idProperty,
				resolver.getValueInternal(idProperty, object, evaluator, objectPath));

		return proxy;
	}
}
