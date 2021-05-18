/*
 * Copyright 2021 the original author or authors.
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

import static org.springframework.data.mongodb.core.convert.ReferenceLookupDelegate.*;

import java.util.Collections;

import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 */
public class DefaultReferenceResolver implements ReferenceResolver {

	private final ReferenceLoader referenceLoader;

	public DefaultReferenceResolver(ReferenceLoader referenceLoader) {
		this.referenceLoader = referenceLoader;
	}

	@Override
	public ReferenceLoader getReferenceLoader() {
		return referenceLoader;
	}

	@Nullable
	@Override
	public Object resolveReference(MongoPersistentProperty property, Object source,
			ReferenceLookupDelegate referenceLookupDelegate, MongoEntityReader entityReader) {

		LookupFunction lookupFunction = (filter, ctx) -> {
			if (property.isCollectionLike() || property.isMap()) {
				return getReferenceLoader().fetchMany(filter, ctx);

			}

			Object target = getReferenceLoader().fetchOne(filter, ctx);
			return target == null ? Collections.emptyList()
					: Collections.singleton(getReferenceLoader().fetchOne(filter, ctx));
		};

		if (isLazyReference(property)) {
			return createLazyLoadingProxy(property, source, referenceLookupDelegate, lookupFunction, entityReader);
		}

		return referenceLookupDelegate.readReference(property, source, lookupFunction, entityReader);
	}

	private Object createLazyLoadingProxy(MongoPersistentProperty property, Object source,
			ReferenceLookupDelegate referenceLookupDelegate, LookupFunction lookupFunction,
			MongoEntityReader entityReader) {
		return new LazyLoadingProxyFactory(referenceLookupDelegate).createLazyLoadingProxy(property, source, lookupFunction,
				entityReader);
	}

	protected boolean isLazyReference(MongoPersistentProperty property) {

		if (property.isDocumentReference()) {
			return property.getDocumentReference().lazy();
		}

		return property.getDBRef() != null && property.getDBRef().lazy();
	}
}
