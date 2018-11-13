/*
 * Copyright 2011-2018 the original author or authors.
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
	private @Nullable boolean dbRefResolved;
	private @Nullable DBRef dbref;
	private @Nullable String fieldName;
	private @Nullable Class<?> fieldType;
	private @Nullable Boolean usePropertyAccess;
	private @Nullable Boolean isTransient;

	/**
	 * Creates a new {@link CachingMongoPersistentProperty}.
	 *
	 * @param property
	 * @param owner
	 * @param simpleTypeHolder
	 * @param fieldNamingStrategy
	 */
	public CachingMongoPersistentProperty(Property property, MongoPersistentEntity<?> owner,
			SimpleTypeHolder simpleTypeHolder, FieldNamingStrategy fieldNamingStrategy) {
		super(property, owner, simpleTypeHolder, fieldNamingStrategy);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.BasicMongoPersistentProperty#isIdProperty()
	 */
	@Override
	public boolean isIdProperty() {

		if (this.isIdProperty == null) {
			this.isIdProperty = super.isIdProperty();
		}

		return this.isIdProperty;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.BasicMongoPersistentProperty#isAssociation()
	 */
	@Override
	public boolean isAssociation() {
		if (this.isAssociation == null) {
			this.isAssociation = super.isAssociation();
		}
		return this.isAssociation;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.BasicMongoPersistentProperty#getFieldName()
	 */
	@Override
	public String getFieldName() {

		if (this.fieldName == null) {
			this.fieldName = super.getFieldName();
		}

		return this.fieldName;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.BasicMongoPersistentProperty#getFieldType()
	 */
	@Override
	public Class<?> getFieldType() {

		if (this.fieldType == null) {
			this.fieldType = super.getFieldType();
		}

		return this.fieldType;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.model.AnnotationBasedPersistentProperty#usePropertyAccess()
	 */
	@Override
	public boolean usePropertyAccess() {

		if (this.usePropertyAccess == null) {
			this.usePropertyAccess = super.usePropertyAccess();
		}

		return this.usePropertyAccess;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.model.AnnotationBasedPersistentProperty#isTransient()
	 */
	@Override
	public boolean isTransient() {

		if (this.isTransient == null) {
			this.isTransient = super.isTransient();
		}

		return this.isTransient;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.BasicMongoPersistentProperty#isDbReference()
	 */
	@Override
	public boolean isDbReference() {
		return getDBRef() != null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.BasicMongoPersistentProperty#getDBRef()
	 */
	@Override
	public DBRef getDBRef() {

		if (!dbRefResolved) {
			this.dbref = super.getDBRef();
			this.dbRefResolved = true;
		}

		return this.dbref;
	}
}
