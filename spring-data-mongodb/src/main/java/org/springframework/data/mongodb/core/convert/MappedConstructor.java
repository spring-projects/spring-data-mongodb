/*
 * Copyright 2012 the original author or authors.
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

import java.util.HashSet;
import java.util.Set;

import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.PreferredConstructor.Parameter;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.context.PersistentPropertyPath;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

/**
 * Abstraction for a {@link PreferredConstructor} alongside mapping information.
 * 
 * @author Oliver Gierke
 */
class MappedConstructor {

	private final Set<MappedConstructor.MappedParameter> parameters;

	/**
	 * Creates a new {@link MappedConstructor} from the given {@link MongoPersistentEntity} and {@link MappingContext}.
	 * 
	 * @param entity must not be {@literal null}.
	 * @param context must not be {@literal null}.
	 */
	public MappedConstructor(MongoPersistentEntity<?> entity,
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> context) {

		Assert.notNull(entity);
		Assert.notNull(context);

		this.parameters = new HashSet<MappedConstructor.MappedParameter>();

		for (Parameter<?> parameter : entity.getPreferredConstructor().getParameters()) {
			parameters.add(new MappedParameter(parameter, entity, context));
		}
	}

	/**
	 * Returns whether the given {@link PersistentProperty} is referenced in a constructor argument of the
	 * {@link PersistentEntity} backing this {@link MappedConstructor}.
	 * 
	 * @param property must not be {@literal null}.
	 * @return
	 */
	public boolean isConstructorParameter(PersistentProperty<?> property) {

		Assert.notNull(property);

		for (MappedConstructor.MappedParameter parameter : parameters) {
			if (parameter.maps(property)) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Returns the {@link MappedParameter} for the given {@link Parameter}.
	 * 
	 * @param parameter must not be {@literal null}.
	 * @return
	 */
	public MappedParameter getFor(Parameter<?> parameter) {

		for (MappedParameter mappedParameter : parameters) {
			if (mappedParameter.parameter.equals(parameter)) {
				return mappedParameter;
			}
		}

		throw new IllegalStateException(String.format("Didn't find a MappedParameter for %s!", parameter.toString()));
	}

	/**
	 * Abstraction of a {@link Parameter} alongside mapping information.
	 * 
	 * @author Oliver Gierke
	 */
	static class MappedParameter {

		private final MongoPersistentProperty property;
		private final Parameter<?> parameter;

		/**
		 * Creates a new {@link MappedParameter} for the given {@link Parameter}, {@link MongoPersistentProperty} and
		 * {@link MappingContext}.
		 * 
		 * @param parameter must not be {@literal null}.
		 * @param entity must not be {@literal null}.
		 * @param context must not be {@literal null}.
		 */
		public MappedParameter(Parameter<?> parameter, MongoPersistentEntity<?> entity,
				MappingContext<? extends MongoPersistentEntity<?>, ? extends MongoPersistentProperty> context) {

			Assert.notNull(parameter);
			Assert.notNull(entity);
			Assert.notNull(context);

			this.parameter = parameter;

			PropertyPath propertyPath = PropertyPath.from(parameter.getName(), entity.getType());
			PersistentPropertyPath<? extends MongoPersistentProperty> path = context.getPersistentPropertyPath(propertyPath);
			this.property = path == null ? null : path.getLeafProperty();
		}

		/**
		 * Returns whether there is a SpEL expression configured for this parameter.
		 * 
		 * @return
		 */
		public boolean hasSpELExpression() {
			return parameter.getKey() != null;
		}

		/**
		 * Returns the field name to be used to lookup the value which in turn shall be converted into the constructor
		 * parameter.
		 * 
		 * @return
		 */
		public String getFieldName() {
			return property.getFieldName();
		}

		/**
		 * Returns the type of the property backing the {@link Parameter}.
		 * 
		 * @return
		 */
		public TypeInformation<?> getPropertyTypeInformation() {
			return property.getTypeInformation();
		}

		/**
		 * Returns whether the given {@link PersistentProperty} is mapped by the parameter.
		 * 
		 * @param property
		 * @return
		 */
		public boolean maps(PersistentProperty<?> property) {
			return this.property.equals(property);
		}
	}
}