/*
 * Copyright 2018-2025 the original author or authors.
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

import java.util.List;

import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

import com.mongodb.DBRef;

/**
 * No-Operation {@link org.springframework.data.mongodb.core.mapping.DBRef} resolver throwing
 * {@link UnsupportedOperationException} when attempting to resolve database references.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.1
 */
public enum NoOpDbRefResolver implements DbRefResolver {

	INSTANCE;

	@Override
	public @Nullable Object resolveDbRef(MongoPersistentProperty property, @Nullable DBRef dbref, DbRefResolverCallback callback,
			DbRefProxyHandler proxyHandler) {

		return handle();
	}

	@Override
	public @Nullable Document fetch(DBRef dbRef) {
		return handle();
	}

	@Override
	public List<Document> bulkFetch(List<DBRef> dbRefs) {
		return handle();
	}

	private <T> T handle() throws UnsupportedOperationException {
		throw new UnsupportedOperationException("DBRef resolution is not supported");
	}

	@Nullable
	@Override
	public Object resolveReference(MongoPersistentProperty property, Object source,
			ReferenceLookupDelegate referenceLookupDelegate, MongoEntityReader entityReader) {
		return null;
	}
}
