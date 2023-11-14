/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;

/**
 * {@link MongoPersistentProperty} caching access to {@link #isIdProperty()} and {@link #getFieldName()}.
 *
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class CachingMongoPersistentProperty extends BasicMongoPersistentProperty {

	private final Lazy<Boolean> isIdProperty = Lazy.of(super::isIdProperty);
	private final Lazy<Boolean> isAssociation = Lazy.of(super::isAssociation);
	private final Lazy<DBRef> dbref = Lazy.of(super::getDBRef);
	private final Lazy<String> fieldName = Lazy.of(super::getFieldName);
	private final Lazy<Boolean> hasExplicitFieldName = Lazy.of(super::hasExplicitFieldName);
	private final Lazy<Boolean> writeNullValues = Lazy.of(super::writeNullValues);
	private final Lazy<Class<?>> fieldType = Lazy.of(super::getFieldType);
	private final Lazy<Boolean> usePropertyAccess = Lazy.of(super::usePropertyAccess);
	private final Lazy<Boolean> isTransient = Lazy.of(super::isTransient);
	private final Lazy<MongoField> mongoField = Lazy.of(super::getMongoField);
	private final Lazy<Boolean> isTextScoreProperty = Lazy.of(super::isTextScoreProperty);
	private final Lazy<Boolean> isLanguageProperty = Lazy.of(super::isLanguageProperty);
	private final Lazy<Boolean> isExplicitLanguageProperty = Lazy.of(super::isExplicitLanguageProperty);
	private final Lazy<DocumentReference> documentReference = Lazy.of(super::getDocumentReference);

	/**
	 * Creates a new {@link CachingMongoPersistentProperty}.
	 *
	 * @param property must not be {@literal null}.
	 * @param owner must not be {@literal null}.
	 * @param simpleTypeHolder must not be {@literal null}.
	 * @param fieldNamingStrategy can be {@literal null}.
	 */
	public CachingMongoPersistentProperty(Property property, MongoPersistentEntity<?> owner,
			SimpleTypeHolder simpleTypeHolder, @Nullable FieldNamingStrategy fieldNamingStrategy) {
		super(property, owner, simpleTypeHolder, fieldNamingStrategy);
	}

	@Override
	public boolean isIdProperty() {
		return isIdProperty.get();
	}

	@Override
	public boolean isAssociation() {
		return isAssociation.get();
	}

	@Override
	public boolean hasExplicitFieldName() {
		return hasExplicitFieldName.get();
	}

	@Override
	public String getFieldName() {
		return fieldName.get();
	}

	@Override
	public boolean writeNullValues() {
		return writeNullValues.get();
	}

	@Override
	public Class<?> getFieldType() {
		return fieldType.get();
	}

	@Override
	public boolean usePropertyAccess() {
		return usePropertyAccess.get();
	}

	@Override
	public boolean isTransient() {
		return isTransient.get();
	}

	@Override
	public boolean isTextScoreProperty() {
		return isTextScoreProperty.get();
	}

	@Override
	public boolean isDbReference() {
		return getDBRef() != null;
	}

	@Override
	public DBRef getDBRef() {
		return dbref.getNullable();
	}

	@Override
	public DocumentReference getDocumentReference() {
		return documentReference.getNullable();
	}

	@Override
	public boolean isLanguageProperty() {
		return isLanguageProperty.get();
	}

	@Override
	public boolean isExplicitLanguageProperty() {
		return isExplicitLanguageProperty.get();
	}

	@Override
	public MongoField getMongoField() {
		return mongoField.get();
	}

}
