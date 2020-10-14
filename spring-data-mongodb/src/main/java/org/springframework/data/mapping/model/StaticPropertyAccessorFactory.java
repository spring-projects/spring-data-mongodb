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

import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 * @since 2020/10
 */
public class StaticPropertyAccessorFactory implements PersistentPropertyAccessorFactory {

	private static final StaticPropertyAccessorFactory INSTANCE = new StaticPropertyAccessorFactory();

	public static StaticPropertyAccessorFactory instance() {
		return INSTANCE;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.model.PersistentPropertyAccessorFactory#getPropertyAccessor(org.springframework.data.mapping.PersistentEntity, java.lang.Object)
	 */
	@Override
	public <T> PersistentPropertyAccessor<T> getPropertyAccessor(PersistentEntity<?, ?> entity, T bean) {

		System.out.println("Obtaining static property acessor for entity " + entity.getName());
		return new StaticPropertyAccessor<>((AccessorFunctionProvider<T>) entity.getTypeInformation(), bean);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.model.PersistentPropertyAccessorFactory#isSupported(org.springframework.data.mapping.PersistentEntity)
	 */
	@Override
	public boolean isSupported(PersistentEntity<?, ?> entity) {

		boolean isStaticTypedEntity = entity.getTypeInformation() instanceof AccessorFunctionProvider;
		System.out.println(entity.getName() + " isStaticTypedEntity: " + isStaticTypedEntity);
		return isStaticTypedEntity;
	}

	static class StaticPropertyAccessor<T> implements PersistentPropertyAccessor<T> {

		T bean;
		AccessorFunctionProvider<T> accessorFunctionProvider;

		public StaticPropertyAccessor(AccessorFunctionProvider<T> accessorFunctionProvider, T bean) {
			this.bean = bean;
			this.accessorFunctionProvider = accessorFunctionProvider;
		}

		@Override
		public void setProperty(PersistentProperty<?> property, @Nullable Object value) {

			if (!accessorFunctionProvider.hasSetFunctionFor(property.getName())) {
				return;
			}

			this.bean = accessorFunctionProvider.getSetFunctionFor(property.getName()).apply(bean, value);
			System.out.println(
					"setting value " + value + " via setter function for " + property.getName() + " resulting in " + bean);
		}

		@Nullable
		@Override
		public Object getProperty(PersistentProperty<?> property) {

			if (!accessorFunctionProvider.hasGetFunctionFor(property.getName())) {
				return null;
			}

			Object value = accessorFunctionProvider.getGetFunctionFor(property.getName()).apply(bean);
			System.out.println("obtaining value " + value + " from getter function for " + property.getName());
			return value;
		}

		@Override
		public T getBean() {
			return this.bean;
		}
	}
}
