/*
 * Copyright 2020. the original author or authors.
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

/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.mapping.model;

import java.util.function.BiFunction;
import java.util.function.Function;

import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.util.StaticTypeInformation;
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 * @since 2020/10
 */
public class StaticPropertyAccessorFactory implements PersistentPropertyAccessorFactory {

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.model.PersistentPropertyAccessorFactory#getPropertyAccessor(org.springframework.data.mapping.PersistentEntity, java.lang.Object)
	 */
	@Override
	public <T> PersistentPropertyAccessor<T> getPropertyAccessor(PersistentEntity<?, ?> entity, T bean) {
		return new StaticPropertyAccessor<>((StaticTypeInformation<T>) entity.getTypeInformation(), bean);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.model.PersistentPropertyAccessorFactory#isSupported(org.springframework.data.mapping.PersistentEntity)
	 */
	@Override
	public boolean isSupported(PersistentEntity<?, ?> entity) {

		boolean isStaticTypedEntity = entity.getTypeInformation() instanceof StaticTypeInformation;
		System.out.println(entity.getName() + " isStaticTypedEntity: " + isStaticTypedEntity);
		return isStaticTypedEntity;
	}

	static class StaticPropertyAccessor<T> implements PersistentPropertyAccessor<T> {

		T bean;
		StaticTypeInformation<T> typeInformation;

		public StaticPropertyAccessor(StaticTypeInformation<T> typeInformation, T bean) {
			this.bean = bean;
			this.typeInformation = typeInformation;
		}

		@Override
		public void setProperty(PersistentProperty<?> property, @Nullable Object value) {

			BiFunction<T, Object, T> setFunction = typeInformation.getSetter().get(property.getName());
			if (setFunction == null) {
				return;
			}
			this.bean = setFunction.apply(bean, value);
		}

		@Nullable
		@Override
		public Object getProperty(PersistentProperty<?> property) {

			Function<T, Object> getFunction = typeInformation.getGetter().get(property.getName());
			if (getFunction == null) {
				return null;
			}
			return getFunction.apply(bean);
		}

		@Override
		public T getBean() {
			return this.bean;
		}
	}
}
