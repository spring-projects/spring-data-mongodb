/*
 * Copyright 2011-2021 the original author or authors.
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
import org.springframework.lang.Nullable;

/**
 * {@link MongoPersistentProperty} caching access to {@link #isIdProperty()} and {@link #getFieldName()}.
 *
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public class CachingMongoPersistentProperty extends BasicMongoPersistentProperty {

	private @Nullable Boolean isIdProperty;
	private @Nullable Boolean isAssociation;
	private boolean dbRefResolved;
	private @Nullable DBRef dbref;
	private @Nullable String fieldName;
	private @Nullable Boolean writeNullValues;
	private @Nullable Class<?> fieldType;
	private @Nullable Boolean usePropertyAccess;
	private @Nullable Boolean isTransient;

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

		if (this.isIdProperty == null) {
			this.isIdProperty = super.isIdProperty();
		}

		return this.isIdProperty;
	}

	@Override
	public boolean isAssociation() {
		if (this.isAssociation == null) {
			this.isAssociation = super.isAssociation();
		}
		return this.isAssociation;
	}

	@Override
	public String getFieldName() {

		if (this.fieldName == null) {
			this.fieldName = super.getFieldName();
		}

		return this.fieldName;
	}

	@Override
	public boolean writeNullValues() {

		if (this.writeNullValues == null) {
			this.writeNullValues = super.writeNullValues();
		}

		return this.writeNullValues;
	}

	@Override
	public Class<?> getFieldType() {

		if (this.fieldType == null) {
			this.fieldType = super.getFieldType();
		}

		return this.fieldType;
	}

	@Override
	public boolean usePropertyAccess() {

		if (this.usePropertyAccess == null) {
			this.usePropertyAccess = super.usePropertyAccess();
		}

		return this.usePropertyAccess;
	}

	@Override
	public boolean isTransient() {

		if (this.isTransient == null) {
			this.isTransient = super.isTransient();
		}

		return this.isTransient;
	}

	@Override
	public boolean isDbReference() {
		return getDBRef() != null;
	}

	@Override
	public DBRef getDBRef() {

		if (!dbRefResolved) {
			this.dbref = super.getDBRef();
			this.dbRefResolved = true;
		}

		return this.dbref;
	}
}
