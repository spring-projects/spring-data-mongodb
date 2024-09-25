/*
 * Copyright 2011-2024 the original author or authors.
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
package org.springframework.data.mongodb.config;

import static org.springframework.data.mongodb.config.BeanNames.*;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.springframework.beans.BeanMetadataElement;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.parsing.BeanComponentDefinition;
import org.springframework.beans.factory.parsing.CompositeComponentDefinition;
import org.springframework.beans.factory.parsing.ReaderContext;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.support.ManagedSet;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.core.type.filter.TypeFilter;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.config.BeanComponentDefinitionBuilder;
import org.springframework.data.mapping.model.CamelCaseAbbreviatingFieldNamingStrategy;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexCreator;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.event.ValidatingMongoEventListener;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * Bean definition parser for the {@code mapping-converter} element.
 *
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Maciej Walkowiak
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Zied Yaich
 * @author Tomasz Forys
 */
public class MappingMongoConverterParser implements BeanDefinitionParser {

	private static final String BASE_PACKAGE = "base-package";
	private static final boolean JSR_303_PRESENT = ClassUtils.isPresent("jakarta.validation.Validator",
			MappingMongoConverterParser.class.getClassLoader());

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.BeanDefinitionParser#parse(org.w3c.dom.Element, org.springframework.beans.factory.xml.ParserContext)
	 */
	public BeanDefinition parse(Element element, ParserContext parserContext) {

		if (parserContext.isNested()) {
			parserContext.getReaderContext().error("Mongo Converter must not be defined as nested bean.", element);
		}

		BeanDefinitionRegistry registry = parserContext.getRegistry();
		String id = element.getAttribute(AbstractBeanDefinitionParser.ID_ATTRIBUTE);
		id = StringUtils.hasText(id) ? id : DEFAULT_CONVERTER_BEAN_NAME;

		boolean autoIndexCreationEnabled = isAutoIndexCreationEnabled(element);

		parserContext.pushContainingComponent(new CompositeComponentDefinition("Mapping Mongo Converter", element));

		BeanDefinition conversionsDefinition = getCustomConversions(element, parserContext);
		String ctxRef = potentiallyCreateMappingContext(element, parserContext, conversionsDefinition, id, autoIndexCreationEnabled);

		// Need a reference to a Mongo instance
		String dbFactoryRef = element.getAttribute("db-factory-ref");
		if (!StringUtils.hasText(dbFactoryRef)) {
			dbFactoryRef = DB_FACTORY_BEAN_NAME;
		}

		// Converter
		BeanDefinitionBuilder converterBuilder = BeanDefinitionBuilder.genericBeanDefinition(MappingMongoConverter.class);
		converterBuilder.addConstructorArgReference(dbFactoryRef);
		converterBuilder.addConstructorArgReference(ctxRef);

		String typeMapperRef = element.getAttribute("type-mapper-ref");
		if (StringUtils.hasText(typeMapperRef)) {
			converterBuilder.addPropertyReference("typeMapper", typeMapperRef);
		}

		if (conversionsDefinition != null) {
			converterBuilder.addPropertyValue("customConversions", conversionsDefinition);
		}

		if (!registry.containsBeanDefinition("indexOperationsProvider")) {

			BeanDefinitionBuilder indexOperationsProviderBuilder = BeanDefinitionBuilder
					.genericBeanDefinition("org.springframework.data.mongodb.core.DefaultIndexOperationsProvider");
			indexOperationsProviderBuilder.addConstructorArgReference(dbFactoryRef);
			indexOperationsProviderBuilder.addConstructorArgValue(BeanDefinitionBuilder
					.genericBeanDefinition(QueryMapper.class).addConstructorArgReference(id).getBeanDefinition());
			parserContext.registerBeanComponent(
					new BeanComponentDefinition(indexOperationsProviderBuilder.getBeanDefinition(), "indexOperationsProvider"));
		}

		if (!registry.containsBeanDefinition(INDEX_HELPER_BEAN_NAME)) {

			BeanDefinitionBuilder indexHelperBuilder = BeanDefinitionBuilder
					.genericBeanDefinition(MongoPersistentEntityIndexCreator.class);
			indexHelperBuilder.addConstructorArgReference(ctxRef);
			indexHelperBuilder.addConstructorArgReference("indexOperationsProvider");
			indexHelperBuilder.addDependsOn(ctxRef);

			parserContext.registerBeanComponent(
					new BeanComponentDefinition(indexHelperBuilder.getBeanDefinition(), INDEX_HELPER_BEAN_NAME));
		}

		BeanDefinition validatingMongoEventListener = potentiallyCreateValidatingMongoEventListener(element, parserContext);

		if (validatingMongoEventListener != null && !registry.containsBeanDefinition(VALIDATING_EVENT_LISTENER_BEAN_NAME)) {
			parserContext.registerBeanComponent(
					new BeanComponentDefinition(validatingMongoEventListener, VALIDATING_EVENT_LISTENER_BEAN_NAME));
		}

		parserContext.registerBeanComponent(new BeanComponentDefinition(converterBuilder.getBeanDefinition(), id));
		parserContext.popAndRegisterContainingComponent();
		return null;
	}

	@Nullable
	private BeanDefinition potentiallyCreateValidatingMongoEventListener(Element element, ParserContext parserContext) {

		String disableValidation = element.getAttribute("disable-validation");
		boolean validationDisabled = StringUtils.hasText(disableValidation) && Boolean.parseBoolean(disableValidation);

		if (!validationDisabled) {

			BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition();
			RuntimeBeanReference validator = getValidator(element, parserContext);

			if (validator != null) {
				builder.getRawBeanDefinition().setBeanClass(ValidatingMongoEventListener.class);
				builder.getRawBeanDefinition().setSource(element);
				builder.addConstructorArgValue(validator);

				return builder.getBeanDefinition();
			}
		}

		return null;
	}

	@Nullable
	private RuntimeBeanReference getValidator(Object source, ParserContext parserContext) {

		if (!JSR_303_PRESENT) {
			return null;
		}

		RootBeanDefinition validatorDef = new RootBeanDefinition(
				"org.springframework.validation.beanvalidation.LocalValidatorFactoryBean");
		validatorDef.setSource(source);
		validatorDef.setRole(BeanDefinition.ROLE_INFRASTRUCTURE);
		String validatorName = parserContext.getReaderContext().registerWithGeneratedName(validatorDef);

		return new RuntimeBeanReference(validatorName);
	}

	private static boolean isAutoIndexCreationEnabled(Element element) {

		String autoIndexCreation = element.getAttribute("auto-index-creation");
		return StringUtils.hasText(autoIndexCreation) && Boolean.parseBoolean(autoIndexCreation);
	}

	/**
	 * Create and register the {@link BeanDefinition} for a {@link MongoMappingContext} if not explicitly referenced by a
	 * given {@literal mapping-context-ref} {@link Element#getAttribute(String) attribuite}.
	 * 
	 * @return the mapping context bean name.
	 * @deprecated since 4.3. Use
	 *             {@link #potentiallyCreateMappingContext(Element, ParserContext, BeanDefinition, String, boolean)}
	 *             instead.
	 */
	@Deprecated(since = "4.3", forRemoval = true)
	public static String potentiallyCreateMappingContext(Element element, ParserContext parserContext,
			@Nullable BeanDefinition conversionsDefinition, @Nullable String converterId) {
		return potentiallyCreateMappingContext(element, parserContext, conversionsDefinition, converterId, false);
	}

	/**
	 * Create and register the {@link BeanDefinition} for a {@link MongoMappingContext} if not explicitly referenced by a
	 * given {@literal mapping-context-ref} {@link Element#getAttribute(String) attribuite}.
	 * 
	 * @return the mapping context bean name.
	 */
	public static String potentiallyCreateMappingContext(Element element, ParserContext parserContext,
			@Nullable BeanDefinition conversionsDefinition, @Nullable String converterId, boolean autoIndexCreation) {

		String ctxRef = element.getAttribute("mapping-context-ref");

		if (StringUtils.hasText(ctxRef)) {
			return ctxRef;
		}

		BeanComponentDefinitionBuilder componentDefinitionBuilder = new BeanComponentDefinitionBuilder(element,
				parserContext);

		BeanDefinitionBuilder mappingContextBuilder = BeanDefinitionBuilder
				.genericBeanDefinition(MongoMappingContext.class);

		Set<String> classesToAdd = getInitialEntityClasses(element);

		if (classesToAdd != null) {
			mappingContextBuilder.addPropertyValue("initialEntitySet", classesToAdd);
		}

		if (conversionsDefinition != null) {
			AbstractBeanDefinition simpleTypesDefinition = new GenericBeanDefinition();
			simpleTypesDefinition.setFactoryBeanName("customConversions");
			simpleTypesDefinition.setFactoryMethodName("getSimpleTypeHolder");

			mappingContextBuilder.addPropertyValue("simpleTypeHolder", simpleTypesDefinition);
		}

		mappingContextBuilder.addPropertyValue("autoIndexCreation", autoIndexCreation);

		parseFieldNamingStrategy(element, parserContext.getReaderContext(), mappingContextBuilder);

		ctxRef = converterId == null || DEFAULT_CONVERTER_BEAN_NAME.equals(converterId) ? MAPPING_CONTEXT_BEAN_NAME
				: converterId + "." + MAPPING_CONTEXT_BEAN_NAME;

		parserContext.registerBeanComponent(componentDefinitionBuilder.getComponent(mappingContextBuilder, ctxRef));
		return ctxRef;
	}

	private static void parseFieldNamingStrategy(Element element, ReaderContext context, BeanDefinitionBuilder builder) {

		String abbreviateFieldNames = element.getAttribute("abbreviate-field-names");
		String fieldNamingStrategy = element.getAttribute("field-naming-strategy-ref");

		boolean fieldNamingStrategyReferenced = StringUtils.hasText(fieldNamingStrategy);
		boolean abbreviationActivated = StringUtils.hasText(abbreviateFieldNames)
				&& Boolean.parseBoolean(abbreviateFieldNames);

		if (fieldNamingStrategyReferenced && abbreviationActivated) {
			context.error("Field name abbreviation cannot be activated if a field-naming-strategy-ref is configured",
					element);
			return;
		}

		Object value = null;

		if ("true".equals(abbreviateFieldNames)) {
			value = new RootBeanDefinition(CamelCaseAbbreviatingFieldNamingStrategy.class);
		} else if (fieldNamingStrategyReferenced) {
			value = new RuntimeBeanReference(fieldNamingStrategy);
		}

		if (value != null) {
			builder.addPropertyValue("fieldNamingStrategy", value);
		}
	}

	@Nullable
	private BeanDefinition getCustomConversions(Element element, ParserContext parserContext) {

		List<Element> customConvertersElements = DomUtils.getChildElementsByTagName(element, "custom-converters");

		if (customConvertersElements.size() == 1) {

			Element customerConvertersElement = customConvertersElements.get(0);
			ManagedList<BeanMetadataElement> converterBeans = new ManagedList<>();
			List<Element> converterElements = DomUtils.getChildElementsByTagName(customerConvertersElement, "converter");

			if (!ObjectUtils.isEmpty(converterElements)) {
				for (Element listenerElement : converterElements) {
					converterBeans.add(parseConverter(listenerElement, parserContext));
				}
			}

			// Scan for Converter and GenericConverter beans in the given base-package
			String packageToScan = customerConvertersElement.getAttribute(BASE_PACKAGE);
			if (StringUtils.hasText(packageToScan)) {
				ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(true);
				provider.addExcludeFilter(new NegatingFilter(new AssignableTypeFilter(Converter.class),
						new AssignableTypeFilter(GenericConverter.class)));

				converterBeans.addAll(provider.findCandidateComponents(packageToScan));
			}

			BeanDefinitionBuilder conversionsBuilder = BeanDefinitionBuilder.rootBeanDefinition(MongoCustomConversions.class);
			conversionsBuilder.addConstructorArgValue(converterBeans);

			AbstractBeanDefinition conversionsBean = conversionsBuilder.getBeanDefinition();
			conversionsBean.setSource(parserContext.extractSource(element));

			parserContext.registerBeanComponent(new BeanComponentDefinition(conversionsBean, "customConversions"));

			return conversionsBean;
		}

		return null;
	}

	@Nullable
	private static Set<String> getInitialEntityClasses(Element element) {

		String basePackage = element.getAttribute(BASE_PACKAGE);

		if (!StringUtils.hasText(basePackage)) {
			return null;
		}

		ClassPathScanningCandidateComponentProvider componentProvider = new ClassPathScanningCandidateComponentProvider(
				false);
		componentProvider.addIncludeFilter(new AnnotationTypeFilter(Document.class));
		componentProvider.addIncludeFilter(new AnnotationTypeFilter(Persistent.class));

		Set<String> classes = new ManagedSet<>();
		for (BeanDefinition candidate : componentProvider.findCandidateComponents(basePackage)) {
			classes.add(candidate.getBeanClassName());
		}

		return classes;
	}

	@Nullable
	public BeanMetadataElement parseConverter(Element element, ParserContext parserContext) {

		String converterRef = element.getAttribute("ref");
		if (StringUtils.hasText(converterRef)) {
			return new RuntimeBeanReference(converterRef);
		}
		Element beanElement = DomUtils.getChildElementByTagName(element, "bean");
		if (beanElement != null) {
			BeanDefinitionHolder beanDef = parserContext.getDelegate().parseBeanDefinitionElement(beanElement);
			beanDef = parserContext.getDelegate().decorateBeanDefinitionIfRequired(beanElement, beanDef);
			return beanDef;
		}

		parserContext.getReaderContext()
				.error("Element <converter> must specify 'ref' or contain a bean definition for the converter", element);
		return null;
	}

	/**
	 * {@link TypeFilter} that returns {@literal false} in case any of the given delegates matches.
	 *
	 * @author Oliver Gierke
	 */
	private static class NegatingFilter implements TypeFilter {

		private final Set<TypeFilter> delegates;

		/**
		 * Creates a new {@link NegatingFilter} with the given delegates.
		 *
		 * @param filters
		 */
		public NegatingFilter(TypeFilter... filters) {

			Assert.notNull(filters, "TypeFilters must not be null");

			this.delegates = Set.of(filters);
		}

		public boolean match(MetadataReader metadataReader, MetadataReaderFactory metadataReaderFactory)
				throws IOException {

			for (TypeFilter delegate : delegates) {
				if (delegate.match(metadataReader, metadataReaderFactory)) {
					return false;
				}
			}

			return true;
		}
	}
}
