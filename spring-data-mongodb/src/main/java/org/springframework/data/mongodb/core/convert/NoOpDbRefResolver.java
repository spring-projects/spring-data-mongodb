/*
 * Copyright 2018 the original author or authors.
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

import java.util.List;

import javax.annotation.Nonnull;

import org.bson.Document;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

import com.mongodb.DBRef;

/**
 * No-Operation {@link org.springframework.data.mongodb.core.mapping.DBRef} resolver throwing
 * {@link UnsupportedOperationException} when attempting to resolve database references.
 *
 * @author Mark Paluch
 * @since 2.1
 */
public enum NoOpDbRefResolver implements DbRefResolver {

	INSTANCE;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.DbRefResolver#resolveDbRef(org.springframework.data.mongodb.core.mapping.MongoPersistentProperty, org.springframework.data.mongodb.core.convert.DbRefResolverCallback)
	 */
	@Override
	@Nullable
	public Object resolveDbRef(@Nonnull MongoPersistentProperty property, @Nonnull DBRef dbref,
			@Nonnull DbRefResolverCallback callback, @Nonnull DbRefProxyHandler proxyHandler) {
		throw new UnsupportedOperationException("DBRef resolution not supported!");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.DbRefResolver#fetch(com.mongodb.DBRef)
	 */
	@Override
	public Document fetch(DBRef dbRef) {
		throw new UnsupportedOperationException("DBRef resolution not supported!");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.DbRefResolver#bulkFetch(java.util.List)
	 */
	@Override
	public List<Document> bulkFetch(List<DBRef> dbRefs) {
		throw new UnsupportedOperationException("DBRef resolution not supported!");
	}
}
