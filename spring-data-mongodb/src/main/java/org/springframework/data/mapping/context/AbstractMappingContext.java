/*
 * Copyright 2011-2020 the original author or authors.
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
package org.springframework.data.mapping.context;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.core.KotlinDetector;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PersistentPropertyPaths;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.model.BeanWrapperPropertyAccessorFactory;
import org.springframework.data.mapping.model.ClassGeneratingPropertyAccessorFactory;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.mapping.model.InstantiationAwarePropertyAccessorFactory;
import org.springframework.data.mapping.model.MutablePersistentEntity;
import org.springframework.data.mapping.model.PersistentPropertyAccessorFactory;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.mapping.model.StaticPropertyAccessorFactory;
import org.springframework.data.spel.EvaluationContextProvider;
import org.springframework.data.spel.ExtensionAwareEvaluationContextProvider;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.KotlinReflectionUtils;
import org.springframework.data.util.Optionals;
import org.springframework.data.util.StaticTypeInformation;
import org.springframework.data.util.Streamable;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.ReflectionUtils.FieldCallback;
import org.springframework.util.ReflectionUtils.FieldFilter;

/**
 * Base class to build mapping metadata and thus create instances of {@link PersistentEntity} and
 * {@link PersistentProperty}.
 * <p>
 * The implementation uses a {@link ReentrantReadWriteLock} to make sure {@link PersistentEntity} are completely
 * populated before accessing them from outside.
 *
 * @param <E> the concrete {@link PersistentEntity} type the {@link MappingContext} implementation creates
 * @param <P> the concrete {@link PersistentProperty} type the {@link MappingContext} implementation creates
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Michael Hunger
 * @author Thomas Darimont
 * @author Tomasz Wysocki
 * @author Mark Paluch
 * @author Mikael Klamra
 * @author Christoph Strobl
 */
public abstract class AbstractMappingContext<E extends MutablePersistentEntity<?, P>, P extends PersistentProperty<P>>
		implements MappingContext<E, P>, ApplicationEventPublisherAware, ApplicationContextAware, InitializingBean {

	private static final boolean IN_NATIVE_IMAGE = System.getProperty("org.graalvm.nativeimage.imagecode") != null;

	private final Optional<E> NONE = Optional.empty();
	private final Map<TypeInformation<?>, Optional<E>> persistentEntities = new HashMap<>();
	private final PersistentPropertyAccessorFactory persistentPropertyAccessorFactory;
	private final PersistentPropertyPathFactory<E, P> persistentPropertyPathFactory;

	private @Nullable ApplicationEventPublisher applicationEventPublisher;
	private EvaluationContextProvider evaluationContextProvider = EvaluationContextProvider.DEFAULT;

	private Set<? extends Class<?>> initialEntitySet = new HashSet<>();
	private boolean strict = false;
	private SimpleTypeHolder simpleTypeHolder = SimpleTypeHolder.DEFAULT;

	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private final Lock read = lock.readLock();
	private final Lock write = lock.writeLock();

	protected AbstractMappingContext() {

		this.persistentPropertyPathFactory = new PersistentPropertyPathFactory<>(this);

		EntityInstantiators instantiators = new EntityInstantiators();
		PersistentPropertyAccessorFactory accessorFactory = IN_NATIVE_IMAGE ? BeanWrapperPropertyAccessorFactory.INSTANCE
				: new ClassGeneratingPropertyAccessorFactory();

		this.persistentPropertyAccessorFactory = new InstantiationAwarePropertyAccessorFactory(accessorFactory,
				instantiators);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationEventPublisherAware#setApplicationEventPublisher(org.springframework.context.ApplicationEventPublisher)
	 */
	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

		this.evaluationContextProvider = new ExtensionAwareEvaluationContextProvider(applicationContext);

		if (applicationEventPublisher == null) {
			this.applicationEventPublisher = applicationContext;
		}
	}

	/**
	 * Sets the {@link Set} of types to populate the context initially.
	 *
	 * @param initialEntitySet
	 */
	public void setInitialEntitySet(Set<? extends Class<?>> initialEntitySet) {
		this.initialEntitySet = initialEntitySet;
	}

	/**
	 * Configures whether the {@link MappingContext} is in strict mode which means, that it will throw
	 * {@link MappingException}s in case one tries to lookup a {@link PersistentEntity} not already in the context. This
	 * defaults to {@literal false} so that unknown types will be transparently added to the MappingContext if not known
	 * in advance.
	 *
	 * @param strict
	 */
	public void setStrict(boolean strict) {
		this.strict = strict;
	}

	/**
	 * Configures the {@link SimpleTypeHolder} to be used by the {@link MappingContext}. Allows customization of what
	 * types will be regarded as simple types and thus not recursively analyzed.
	 *
	 * @param simpleTypes must not be {@literal null}.
	 */
	public void setSimpleTypeHolder(SimpleTypeHolder simpleTypes) {

		Assert.notNull(simpleTypes, "SimpleTypeHolder must not be null!");

		this.simpleTypeHolder = simpleTypes;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.model.MappingContext#getPersistentEntities()
	 */
	@Override
	public Collection<E> getPersistentEntities() {

		try {

			read.lock();

			return persistentEntities.values().stream()//
					.flatMap(Optionals::toStream)//
					.collect(Collectors.toSet());

		} finally {
			read.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.model.MappingContext#getPersistentEntity(java.lang.Class)
	 */
	@Nullable
	public E getPersistentEntity(Class<?> type) {
		return getPersistentEntity(ClassTypeInformation.from(type));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.context.MappingContext#hasPersistentEntityFor(java.lang.Class)
	 */
	@Override
	public boolean hasPersistentEntityFor(Class<?> type) {

		Assert.notNull(type, "Type must not be null!");

		Optional<E> entity = persistentEntities.get(ClassTypeInformation.from(type));

		return entity == null ? false : entity.isPresent();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.model.MappingContext#getPersistentEntity(org.springframework.data.util.TypeInformation)
	 */
	@Nullable
	@Override
	public E getPersistentEntity(TypeInformation<?> type) {

		Assert.notNull(type, "Type must not be null!");

		try {

			read.lock();

			Optional<E> entity = persistentEntities.get(type);

			if (entity != null) {
				return entity.orElse(null);
			}

		} finally {
			read.unlock();
		}

		if (!shouldCreatePersistentEntityFor(type)) {

			try {
				write.lock();
				persistentEntities.put(type, NONE);
			} finally {
				write.unlock();
			}

			return null;
		}

		if (strict) {
			throw new MappingException("Unknown persistent entity " + type);
		}

		return addPersistentEntity(type).orElse(null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.context.MappingContext#getPersistentEntity(org.springframework.data.mapping.PersistentProperty)
	 */
	@Nullable
	@Override
	public E getPersistentEntity(P persistentProperty) {

		Assert.notNull(persistentProperty, "PersistentProperty must not be null!");

		if (!persistentProperty.isEntity()) {
			return null;
		}

		TypeInformation<?> typeInfo = persistentProperty.getTypeInformation();
		return getPersistentEntity(typeInfo.getRequiredActualType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.context.MappingContext#getPersistentPropertyPath(java.lang.Class, java.lang.String)
	 */
	@Override
	public PersistentPropertyPath<P> getPersistentPropertyPath(PropertyPath propertyPath) {
		return persistentPropertyPathFactory.from(propertyPath);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.context.MappingContext#getPersistentPropertyPath(java.lang.String, java.lang.Class)
	 */
	@Override
	public PersistentPropertyPath<P> getPersistentPropertyPath(String propertyPath, Class<?> type) {
		return persistentPropertyPathFactory.from(type, propertyPath);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.context.MappingContext#findPersistentPropertyPath(java.lang.Class, java.util.function.Predicate)
	 */
	@Override
	public <T> PersistentPropertyPaths<T, P> findPersistentPropertyPaths(Class<T> type, Predicate<? super P> predicate) {

		Assert.notNull(type, "Type must not be null!");
		Assert.notNull(predicate, "Selection predicate must not be null!");

		return doFindPersistentPropertyPaths(type, predicate, it -> !it.isAssociation());
	}

	/**
	 * Actually looks up the {@link PersistentPropertyPaths} for the given type, selection predicate and traversal guard.
	 * Primary purpose is to allow sub-types to alter the default traversal guard, e.g. used by
	 * {@link #findPersistentPropertyPaths(Class, Predicate)}.
	 *
	 * @param type will never be {@literal null}.
	 * @param predicate will never be {@literal null}.
	 * @param traversalGuard will never be {@literal null}.
	 * @return will never be {@literal null}.
	 * @see #findPersistentPropertyPaths(Class, Predicate)
	 */
	protected final <T> PersistentPropertyPaths<T, P> doFindPersistentPropertyPaths(Class<T> type,
			Predicate<? super P> predicate, Predicate<P> traversalGuard) {
		return persistentPropertyPathFactory.from(ClassTypeInformation.from(type), predicate, traversalGuard);
	}

	/**
	 * Adds the given type to the {@link MappingContext}.
	 *
	 * @param type must not be {@literal null}.
	 * @return
	 */
	protected Optional<E> addPersistentEntity(Class<?> type) {
		return addPersistentEntity(ClassTypeInformation.from(type));
	}

	/**
	 * Adds the given {@link TypeInformation} to the {@link MappingContext}.
	 *
	 * @param typeInformation must not be {@literal null}.
	 * @return
	 */
	protected Optional<E> addPersistentEntity(TypeInformation<?> typeInformation) {

		Assert.notNull(typeInformation, "TypeInformation must not be null!");

		try {

			read.lock();

			Optional<E> persistentEntity = persistentEntities.get(typeInformation);

			if (persistentEntity != null) {
				return persistentEntity;
			}

		} finally {
			read.unlock();
		}

		Class<?> type = typeInformation.getType();
		E entity = null;

		try {

			write.lock();

			entity = createPersistentEntity(typeInformation);

			entity.setEvaluationContextProvider(evaluationContextProvider);

			// Eagerly cache the entity as we might have to find it during recursive lookups.
			persistentEntities.put(typeInformation, Optional.of(entity));

			if (typeInformation instanceof StaticTypeInformation<?>) {

				final E pEntity = entity;
				((StaticTypeInformation<?>) typeInformation).doWithFields((fieldName, field) -> {

					System.out.println("Creating PersistentProperty for " + fieldName + " via static configuration.");
					P target = createPersistentProperty(
							Property.of(field.getTypeInformation(), fieldName, field.getAnnotations()), pEntity, simpleTypeHolder);
					pEntity.addPersistentProperty(target);

				});
				pEntity.setPersistentPropertyAccessorFactory(StaticPropertyAccessorFactory.instance());
				return Optional.of(pEntity);
			}

			PropertyDescriptor[] pds = BeanUtils.getPropertyDescriptors(type);

			final Map<String, PropertyDescriptor> descriptors = new HashMap<>();
			for (PropertyDescriptor descriptor : pds) {
				descriptors.put(descriptor.getName(), descriptor);
			}

			try {

				PersistentPropertyCreator persistentPropertyCreator = new PersistentPropertyCreator(entity, descriptors);
				ReflectionUtils.doWithFields(type, persistentPropertyCreator, PersistentPropertyFilter.INSTANCE);
				persistentPropertyCreator.addPropertiesForRemainingDescriptors();

				entity.verify();

				if (persistentPropertyAccessorFactory.isSupported(entity)) {
					entity.setPersistentPropertyAccessorFactory(persistentPropertyAccessorFactory);
				}

			} catch (RuntimeException e) {
				persistentEntities.remove(typeInformation);
				throw e;
			}

		} catch (BeansException e) {
			throw new MappingException(e.getMessage(), e);
		} finally {
			write.unlock();
		}

		// Inform listeners
		if (applicationEventPublisher != null && entity != null) {
			applicationEventPublisher.publishEvent(new MappingContextEvent<>(this, entity));
		}

		return Optional.of(entity);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.context.PersistentEntityAware#getManagedTypes()
	 */
	@Override
	public Collection<TypeInformation<?>> getManagedTypes() {

		try {

			read.lock();
			return Collections.unmodifiableSet(new HashSet<>(persistentEntities.keySet()));

		} finally {
			read.unlock();
		}
	}

	/**
	 * Creates the concrete {@link PersistentEntity} instance.
	 *
	 * @param <T>
	 * @param typeInformation
	 * @return
	 */
	protected abstract <T> E createPersistentEntity(TypeInformation<T> typeInformation);

	/**
	 * Creates the concrete instance of {@link PersistentProperty}.
	 *
	 * @param property
	 * @param owner
	 * @param simpleTypeHolder
	 * @return
	 */
	protected abstract P createPersistentProperty(Property property, E owner, SimpleTypeHolder simpleTypeHolder);

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() {
		initialize();
	}

	/**
	 * Initializes the mapping context. Will add the types configured through {@link #setInitialEntitySet(Set)} to the
	 * context.
	 */
	public void initialize() {
		initialEntitySet.forEach(this::addPersistentEntity);
	}

	/**
	 * Returns whether a {@link PersistentEntity} instance should be created for the given {@link TypeInformation}. By
	 * default this will reject all types considered simple and non-supported Kotlin classes, but it might be necessary to
	 * tweak that in case you have registered custom converters for top level types (which renders them to be considered
	 * simple) but still need meta-information about them.
	 * <p/>
	 *
	 * @param type will never be {@literal null}.
	 * @return
	 */
	protected boolean shouldCreatePersistentEntityFor(TypeInformation<?> type) {

		if (simpleTypeHolder.isSimpleType(type.getType())) {
			return false;
		}

		return !KotlinDetector.isKotlinType(type.getType()) || KotlinReflectionUtils.isSupportedKotlinClass(type.getType());
	}

	/**
	 * {@link FieldCallback} to create {@link PersistentProperty} instances.
	 *
	 * @author Oliver Gierke
	 */
	private final class PersistentPropertyCreator implements FieldCallback {

		private final E entity;
		private final Map<String, PropertyDescriptor> descriptors;
		private final Map<String, PropertyDescriptor> remainingDescriptors;

		public PersistentPropertyCreator(E entity, Map<String, PropertyDescriptor> descriptors) {
			this(entity, descriptors, descriptors);
		}

		private PersistentPropertyCreator(E entity, Map<String, PropertyDescriptor> descriptors,
				Map<String, PropertyDescriptor> remainingDescriptors) {
			this.entity = entity;
			this.descriptors = descriptors;
			this.remainingDescriptors = remainingDescriptors;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.util.ReflectionUtils.FieldCallback#doWith(java.lang.reflect.Field)
		 */
		public void doWith(Field field) {

			String fieldName = field.getName();
			TypeInformation<?> type = entity.getTypeInformation();

			ReflectionUtils.makeAccessible(field);

			Property property = Optional.ofNullable(descriptors.get(fieldName))//
					.map(it -> Property.of(type, field, it))//
					.orElseGet(() -> Property.of(type, field));

			createAndRegisterProperty(property);

			this.remainingDescriptors.remove(fieldName);
		}

		/**
		 * Adds {@link PersistentProperty} instances for all suitable {@link PropertyDescriptor}s without a backing
		 * {@link Field}.
		 *
		 * @see PersistentPropertyFilter
		 */
		public void addPropertiesForRemainingDescriptors() {

			remainingDescriptors.values().stream() //
					.filter(Property::supportsStandalone) //
					.map(it -> Property.of(entity.getTypeInformation(), it)) //
					.filter(PersistentPropertyFilter.INSTANCE::matches) //
					.forEach(this::createAndRegisterProperty);
		}

		private void createAndRegisterProperty(Property input) {

			P property = createPersistentProperty(input, entity, simpleTypeHolder);

			if (property.isTransient()) {
				return;
			}

			if (!input.isFieldBacked() && !property.usePropertyAccess()) {
				return;
			}

			entity.addPersistentProperty(property);

			if (property.isAssociation()) {
				entity.addAssociation(property.getRequiredAssociation());
			}

			if (entity.getType().equals(property.getRawType())) {
				return;
			}

			property.getPersistentEntityTypes().forEach(AbstractMappingContext.this::addPersistentEntity);
		}
	}

	/**
	 * Filter rejecting static fields as well as artificially introduced ones. See
	 * {@link PersistentPropertyFilter#UNMAPPED_PROPERTIES} for details.
	 *
	 * @author Oliver Gierke
	 */
	static enum PersistentPropertyFilter implements FieldFilter {

		INSTANCE;

		private static final Streamable<PropertyMatch> UNMAPPED_PROPERTIES;

		static {

			Set<PropertyMatch> matches = new HashSet<>();
			matches.add(new PropertyMatch("class", null));
			matches.add(new PropertyMatch("this\\$.*", null));
			matches.add(new PropertyMatch("metaClass", "groovy.lang.MetaClass"));

			UNMAPPED_PROPERTIES = Streamable.of(matches);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.util.ReflectionUtils.FieldFilter#matches(java.lang.reflect.Field)
		 */
		public boolean matches(Field field) {

			if (Modifier.isStatic(field.getModifiers())) {
				return false;
			}

			return !UNMAPPED_PROPERTIES.stream()//
					.anyMatch(it -> it.matches(field.getName(), field.getType()));
		}

		/**
		 * Returns whether the given {@link PropertyDescriptor} is one to create a {@link PersistentProperty} for.
		 *
		 * @param property must not be {@literal null}.
		 * @return
		 */
		public boolean matches(Property property) {

			Assert.notNull(property, "Property must not be null!");

			if (!property.hasAccessor()) {
				return false;
			}

			return !UNMAPPED_PROPERTIES.stream()//
					.anyMatch(it -> it.matches(property.getName(), property.getType()));
		}

		/**
		 * Value object to help defining property exclusion based on name patterns and types.
		 *
		 * @since 1.4
		 * @author Oliver Gierke
		 */
		static class PropertyMatch {

			private final @Nullable String namePattern;
			private final @Nullable String typeName;

			/**
			 * Creates a new {@link PropertyMatch} for the given name pattern and type name. At least one of the parameters
			 * must not be {@literal null}.
			 *
			 * @param namePattern a regex pattern to match field names, can be {@literal null}.
			 * @param typeName the name of the type to exclude, can be {@literal null}.
			 */
			public PropertyMatch(@Nullable String namePattern, @Nullable String typeName) {

				Assert.isTrue(!(namePattern == null && typeName == null), "Either name pattern or type name must be given!");

				this.namePattern = namePattern;
				this.typeName = typeName;
			}

			/**
			 * Returns whether the given {@link Field} matches the defined {@link PropertyMatch}.
			 *
			 * @param name must not be {@literal null}.
			 * @param type must not be {@literal null}.
			 * @return
			 */
			public boolean matches(String name, Class<?> type) {

				Assert.notNull(name, "Name must not be null!");
				Assert.notNull(type, "Type must not be null!");

				if (namePattern != null && !name.matches(namePattern)) {
					return false;
				}

				if (typeName != null && !type.getName().equals(typeName)) {
					return false;
				}

				return true;
			}
		}
	}
}
