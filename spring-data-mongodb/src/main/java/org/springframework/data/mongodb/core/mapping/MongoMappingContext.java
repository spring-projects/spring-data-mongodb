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

import java.util.AbstractMap;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.NullableWrapperConverters;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

/**
 * Default implementation of a {@link MappingContext} for MongoDB using {@link BasicMongoPersistentEntity} and
 * {@link BasicMongoPersistentProperty} as primary abstractions.
 *
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public class MongoMappingContext extends AbstractMappingContext<MongoPersistentEntity<?>, MongoPersistentProperty>
		implements ApplicationContextAware {

	private static final FieldNamingStrategy DEFAULT_NAMING_STRATEGY = PropertyNameFieldNamingStrategy.INSTANCE;

	private FieldNamingStrategy fieldNamingStrategy = DEFAULT_NAMING_STRATEGY;
	private boolean autoIndexCreation = false;

	@Nullable
	private ApplicationContext applicationContext;

	/**
	 * Creates a new {@link MongoMappingContext}.
	 */
	public MongoMappingContext() {
		setSimpleTypeHolder(MongoSimpleTypes.HOLDER);
	}

	/**
	 * Configures the {@link FieldNamingStrategy} to be used to determine the field name if no manual mapping is applied.
	 * Defaults to a strategy using the plain property name.
	 *
	 * @param fieldNamingStrategy the {@link FieldNamingStrategy} to be used to determine the field name if no manual
	 *          mapping is applied.
	 */
	public void setFieldNamingStrategy(@Nullable FieldNamingStrategy fieldNamingStrategy) {
		this.fieldNamingStrategy = fieldNamingStrategy == null ? DEFAULT_NAMING_STRATEGY : fieldNamingStrategy;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#shouldCreatePersistentEntityFor(org.springframework.data.util.TypeInformation)
	 */
	@Override
	protected boolean shouldCreatePersistentEntityFor(TypeInformation<?> type) {

		if (NullableWrapperConverters.supports(type.getType())) {
			return false;
		}

		return !MongoSimpleTypes.HOLDER.isSimpleType(type.getType()) && !AbstractMap.class.isAssignableFrom(type.getType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.AbstractMappingContext#createPersistentProperty(java.lang.reflect.Field, java.beans.PropertyDescriptor, org.springframework.data.mapping.MutablePersistentEntity, org.springframework.data.mapping.SimpleTypeHolder)
	 */
	@Override
	public MongoPersistentProperty createPersistentProperty(Property property, MongoPersistentEntity<?> owner,
			SimpleTypeHolder simpleTypeHolder) {
		return new CachingMongoPersistentProperty(property, owner, simpleTypeHolder, fieldNamingStrategy);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.BasicMappingContext#createPersistentEntity(org.springframework.data.util.TypeInformation, org.springframework.data.mapping.model.MappingContext)
	 */
	@Override
	protected <T> BasicMongoPersistentEntity<T> createPersistentEntity(TypeInformation<T> typeInformation) {
		return new BasicMongoPersistentEntity<>(typeInformation);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

		this.applicationContext = applicationContext;
		super.setApplicationContext(applicationContext);
	}

	/**
	 * Returns whether auto-index creation is enabled or disabled. <br />
	 * <strong>NOTE:</strong>Index creation should happen at a well-defined time that is ideally controlled by the
	 * application itself.
	 *
	 * @return {@literal true} when auto-index creation is enabled; {@literal false} otherwise. <br />
	 *         <strong>INFO</strong>: As of 3.x the default will is set to {@literal false} was {@literal true} in 2.x.
	 * @since 2.2
	 * @see org.springframework.data.mongodb.core.index.Indexed
	 */
	public boolean isAutoIndexCreation() {
		return autoIndexCreation;
	}

	/**
	 * Enables/disables auto-index creation. <br />
	 * <strong>NOTE:</strong>Index creation should happen at a well-defined time that is ideally controlled by the
	 * application itself.
	 *
	 * @param autoCreateIndexes set to {@literal true} to enable auto-index creation.
	 * @since 2.2
	 * @see org.springframework.data.mongodb.core.index.Indexed
	 */
	public void setAutoIndexCreation(boolean autoCreateIndexes) {
		this.autoIndexCreation = autoCreateIndexes;
	}

	@Nullable
	@Override
	public MongoPersistentEntity<?> getPersistentEntity(MongoPersistentProperty persistentProperty) {

		MongoPersistentEntity<?> entity = super.getPersistentEntity(persistentProperty);

		if (entity == null || !persistentProperty.isUnwrapped()) {
			return entity;
		}

		return new UnwrappedMongoPersistentEntity<>(entity, new UnwrapEntityContext(persistentProperty));
	}

	@Nullable
	public ApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

}
