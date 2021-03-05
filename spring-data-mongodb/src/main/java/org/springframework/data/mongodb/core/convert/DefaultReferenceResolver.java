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

import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.mongodb.core.convert.ReferenceLoader.ReferenceFilter;
import org.springframework.data.mongodb.core.mapping.DocumentReference;
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
	public Object resolveReference(MongoPersistentProperty property, Object source, ReferenceReader referenceReader,
			BiFunction<ReferenceContext, ReferenceFilter, Stream<Document>> lookupFunction) {

		if (isLazyReference(property)) {
			return createLazyLoadingProxy(property, source, referenceReader, lookupFunction);
		}

		return referenceReader.readReference(property, source, lookupFunction);
	}

	private Object createLazyLoadingProxy(MongoPersistentProperty property, Object source,
			ReferenceReader referenceReader, BiFunction<ReferenceContext, ReferenceFilter, Stream<Document>> lookupFunction) {
		return new LazyLoadingProxyGenerator(referenceReader).createLazyLoadingProxy(property, source, lookupFunction);
	}

	protected boolean isLazyReference(MongoPersistentProperty property) {

		if (property.findAnnotation(DocumentReference.class) != null) {
			return property.findAnnotation(DocumentReference.class).lazy();
		}

		return property.getDBRef() != null && property.getDBRef().lazy();
	}
}
