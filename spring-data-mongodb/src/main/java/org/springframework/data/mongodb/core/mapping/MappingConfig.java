/*
 * Copyright 2023 the original author or authors.
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

import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.data.mapping.Parameter;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.SimplePropertyHandler;
import org.springframework.data.mapping.model.EntityInstantiator;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.MethodInvocationRecorder;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 * @since 2023/06
 */
public class MappingConfig {

	private final Map<Class, EntityConfig<?>> entityConfigMap;

	MappingConfig(Map<Class, EntityConfig<?>> entityConfigMap) {
		this.entityConfigMap = entityConfigMap;
	}

	public static MappingConfig none() {
		return new MappingConfig(Collections.emptyMap());
	}

	public static MappingConfig mappingRules(Consumer<MappingRuleCustomizer> customizer) {
		MappingConfig mappingConfig = new MappingConfig(new HashMap<>());
		customizer.accept(new MappingRuleCustomizer() {
			@Override
			public <T> MappingRuleCustomizer add(Class<T> type, Consumer<EntityConfig<T>> cfg) {

				EntityConfig<T> entityConfig = (EntityConfig<T>) mappingConfig.entityConfigMap.computeIfAbsent(type,
						(it) -> EntityConfig.configure(it));
				cfg.accept(entityConfig);
				return this;
			}
		});
		return mappingConfig;
	}

	public interface MappingRuleCustomizer {
		<T> MappingRuleCustomizer add(Class<T> type, Consumer<EntityConfig<T>> cfg);
	}

	@Nullable
	public <T> EntityConfig<T> getEntityConfig(Class<T> type) {
		return (EntityConfig<T>) entityConfigMap.get(type);
	}

	public static class EntityConfig<T> {

		private final Class<T> type;

		@Nullable private Supplier<String> collectionName;
		Map<String, PropertyConfig<T, ?>> propertyConfigMap = new HashMap<>();
		EntityInstantiator instantiator;

		public EntityConfig(Class<T> type) {
			this.type = type;
		}

		public static <T, P> EntityConfig<T> configure(Class<T> type) {
			return new EntityConfig<>(type);
		}

		public <P> EntityConfig<T> define(String name, Consumer<PropertyConfig<T, P>> cfg) {

			PropertyConfig<T, P> config = (PropertyConfig<T, P>) propertyConfigMap.computeIfAbsent(name,
					(key) -> new PropertyConfig<>(this.type, key));
			cfg.accept(config);
			return this;
		}

		public <P> EntityConfig<T> define(Function<T, P> property, Consumer<PropertyConfig<T, P>> cfg) {

			String propertyName = MethodInvocationRecorder.forProxyOf(type).record(property).getPropertyPath()
					.orElseThrow(() -> new IllegalArgumentException("Cannot obtain property name"));

			return define(propertyName, cfg);
		}

		public EntityConfig<T> namespace(String name) {
			return namespace(() -> name);
		}

		public EntityConfig<T> namespace(Supplier<String> name) {
			this.collectionName = name;
			return this;
		}

		boolean isIdProperty(PersistentProperty<?> property) {
			PropertyConfig<T, ?> propertyConfig = propertyConfigMap.get(property.getName());
			if (propertyConfig == null) {
				return false;
			}

			return propertyConfig.isId();
		}

		String collectionNameOrDefault(Supplier<String> fallback) {
			return collectionName != null ? collectionName.get() : fallback.get();
		}

		public EntityInstantiator getInstantiator() {
			return instantiator;
		}

		public EntityConfig<T> entityCreator(Function<Arguments<T>, T> createFunction) {

			instantiator = new EntityInstantiator() {

				@Override
				public <T, E extends PersistentEntity<? extends T, P>, P extends PersistentProperty<P>> T createInstance(
						E entity, ParameterValueProvider<P> provider) {
					Map<String, Object> targetMap = new HashMap<>();


					PropertyValueProvider pvv = provider instanceof PropertyValueProvider pvp ? pvp : new PropertyValueProvider<P>() {
						@Nullable
						@Override
						public <T> T getPropertyValue(P property) {
							Parameter parameter = new Parameter<>(property.getName(), (TypeInformation) property.getTypeInformation(),
									new Annotation[] {}, null);
							return (T) provider.getParameterValue(parameter);
						}
					};

					entity.doWithProperties((SimplePropertyHandler) property -> {
						targetMap.put(property.getName(), pvv.getPropertyValue(property));
					});

					return (T) createFunction.apply(new Arguments() {

						private Map<Function, String> resolvedName = new HashMap<>();

						@Override
						public Object get(String arg) {
							return targetMap.get(arg);
						}

						@Override
						public Class getType() {
							return entity.getType();
						}

						@Override
						public Object get(Function property) {

							String name = resolvedName.computeIfAbsent(property, key -> (String) MethodInvocationRecorder.forProxyOf(getType()).record(property).getPropertyPath().orElse(""));
							return get(name);
						}
					});
				}
			};
			return this;
		}

		public interface Arguments<T> {

			<V> V get(String arg);

			default <V> V get(Function<T, V> property) {
				String propertyName = MethodInvocationRecorder.forProxyOf(getType()).record(property).getPropertyPath()
						.orElseThrow(() -> new IllegalArgumentException("Cannot obtain property name"));

				return get(propertyName);
			}

			Class<T> getType();
		}
	}

	public static class PropertyConfig<T, P> {

		private final Class<T> owingType;
		private final String propertyName;
		private String fieldName;
		private boolean isId;
		private boolean isTransient;

		public PropertyConfig(Class<T> owingType, String propertyName) {
			this.owingType = owingType;
			this.propertyName = propertyName;
		}

		public PropertyConfig<T, P> useAsId() {
			this.isId = true;
			return this;
		}

		public boolean isId() {
			return isId;
		}

		public PropertyConfig<T, P> setTransient() {
			this.isTransient = true;
			return this;
		}

		public PropertyConfig<T, P> mappedName(String fieldName) {
			this.fieldName = fieldName;
			return this;
		}

		public String getTargetName() {
			return this.fieldName;
		}
	}

}
