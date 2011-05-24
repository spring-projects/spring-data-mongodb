/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.document.mongodb.config;

import static org.springframework.data.document.mongodb.config.BeanNames.*;

import java.util.List;
import java.util.Set;

import org.springframework.beans.BeanMetadataElement;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.support.ManagedSet;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.document.mongodb.convert.MappingMongoConverter;
import org.springframework.data.document.mongodb.mapping.Document;
import org.springframework.data.document.mongodb.mapping.MongoMappingContext;
import org.springframework.data.document.mongodb.mapping.MongoPersistentEntityIndexCreator;
import org.springframework.data.mapping.context.MappingContextAwareBeanPostProcessor;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 * @author Oliver Gierke
 */
public class MappingMongoConverterParser extends AbstractBeanDefinitionParser {

	private static final String BASE_PACKAGE = "base-package";

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {
		String id = super.resolveId(element, definition, parserContext);
		return StringUtils.hasText(id) ? id : "mappingConverter";
	}

	@Override
	protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
		BeanDefinitionRegistry registry = parserContext.getRegistry();

		String ctxRef = element.getAttribute("mapping-context-ref");
		if (!StringUtils.hasText(ctxRef)) {
			BeanDefinitionBuilder mappingContextBuilder = BeanDefinitionBuilder
					.genericBeanDefinition(MongoMappingContext.class);

			Set<String> classesToAdd = getInititalEntityClasses(element, mappingContextBuilder);
			if (classesToAdd != null) {
				mappingContextBuilder.addPropertyValue("initialEntitySet", classesToAdd);
			}

			registry.registerBeanDefinition(MAPPING_CONTEXT, mappingContextBuilder.getBeanDefinition());
			ctxRef = MAPPING_CONTEXT;
		}

		try {
			registry.getBeanDefinition(POST_PROCESSOR);
		} catch (NoSuchBeanDefinitionException ignored) {
			BeanDefinitionBuilder postProcBuilder = BeanDefinitionBuilder
					.genericBeanDefinition(MappingContextAwareBeanPostProcessor.class);
			postProcBuilder.addPropertyValue("mappingContextBeanName", ctxRef);
			registry.registerBeanDefinition(POST_PROCESSOR, postProcBuilder.getBeanDefinition());
		}

		// Need a reference to a Mongo instance
		String dbFactoryRef = element.getAttribute("db-factory-ref");
		if (!StringUtils.hasText(dbFactoryRef)) {
			dbFactoryRef = DB_FACTORY;
		}
		
		
		BeanDefinitionBuilder converterBuilder = BeanDefinitionBuilder.genericBeanDefinition(MappingMongoConverter.class);
		converterBuilder.addConstructorArgReference(dbFactoryRef);
		converterBuilder.addConstructorArgReference(ctxRef);


		try {
			registry.getBeanDefinition(INDEX_HELPER);
		} catch (NoSuchBeanDefinitionException ignored) {
			if (!StringUtils.hasText(dbFactoryRef)) {
				dbFactoryRef = DB_FACTORY;
			}
			BeanDefinitionBuilder indexHelperBuilder = BeanDefinitionBuilder.genericBeanDefinition(MongoPersistentEntityIndexCreator.class);
			indexHelperBuilder.addConstructorArgValue(new RuntimeBeanReference(ctxRef));
			indexHelperBuilder.addConstructorArgValue(new RuntimeBeanReference(dbFactoryRef));
			registry.registerBeanDefinition(INDEX_HELPER, indexHelperBuilder.getBeanDefinition());
		}

		List<Element> customConvertersElements = DomUtils.getChildElementsByTagName(element, "custom-converters");
		if (customConvertersElements.size() == 1) {
			Element customerConvertersElement = customConvertersElements.get(0);
			ManagedList<BeanMetadataElement> converterBeans = new ManagedList<BeanMetadataElement>();
			List<Element> listenerElements = DomUtils.getChildElementsByTagName(customerConvertersElement, "converter");
			if (listenerElements != null) {
				for (Element listenerElement : listenerElements) {
					converterBeans.add(parseConverter(listenerElement, parserContext));
				}
			}
			converterBuilder.addPropertyValue("customConverters", converterBeans);
		}

		return converterBuilder.getBeanDefinition();
	}

	public Set<String> getInititalEntityClasses(Element element, BeanDefinitionBuilder builder) {

		String basePackage = element.getAttribute(BASE_PACKAGE);

		if (!StringUtils.hasText(basePackage)) {
			return null;
		}

		ClassPathScanningCandidateComponentProvider componentProvider = new ClassPathScanningCandidateComponentProvider(
				false);
		componentProvider.addIncludeFilter(new AnnotationTypeFilter(Document.class));
		componentProvider.addIncludeFilter(new AnnotationTypeFilter(Persistent.class));

		Set<String> classes = new ManagedSet<String>();
		for (BeanDefinition candidate : componentProvider.findCandidateComponents(basePackage)) {
			classes.add(candidate.getBeanClassName());
		}

		return classes;
	}

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

		parserContext.getReaderContext().error(
				"Element <converter> must specify 'ref' or contain a bean definition for the converter", element);
		return null;
	}
}
