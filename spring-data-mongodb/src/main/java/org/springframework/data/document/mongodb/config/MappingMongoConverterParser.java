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

import java.util.Set;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
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
import org.w3c.dom.Element;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 * @author Oliver Gierke
 */
public class MappingMongoConverterParser extends AbstractBeanDefinitionParser {

	static final String MAPPING_CONTEXT = "mappingContext";

	private static final String INDEX_HELPER = "indexCreationHelper";
	private static final String TEMPLATE = "mongoTemplate";
	private static final String POST_PROCESSOR = "mappingContextAwareBeanPostProcessor";

	private static final String BASE_PACKAGE = "base-package";

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext) throws BeanDefinitionStoreException {
		String id = super.resolveId(element, definition, parserContext);
		return StringUtils.hasText(id) ? id : "mappingConverter";
	}

	@Override
	protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
		BeanDefinitionRegistry registry = parserContext.getRegistry();

		String ctxRef = element.getAttribute("mapping-context-ref");
		if (!StringUtils.hasText(ctxRef)) {
			BeanDefinitionBuilder mappingContextBuilder = BeanDefinitionBuilder.genericBeanDefinition(MongoMappingContext.class);

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
			BeanDefinitionBuilder postProcBuilder = BeanDefinitionBuilder.genericBeanDefinition(MappingContextAwareBeanPostProcessor.class);
			postProcBuilder.addPropertyValue("mappingContextBeanName", ctxRef);
			registry.registerBeanDefinition(POST_PROCESSOR, postProcBuilder.getBeanDefinition());
		}

		BeanDefinitionBuilder converterBuilder = BeanDefinitionBuilder.genericBeanDefinition(MappingMongoConverter.class);
		converterBuilder.addConstructorArgReference(ctxRef);

		// Need a reference to a Mongo instance
		String mongoRef = element.getAttribute("mongo-ref");
		converterBuilder.addPropertyReference("mongo", StringUtils.hasText(mongoRef) ? mongoRef : "mongo");

		try {
			registry.getBeanDefinition(INDEX_HELPER);
		} catch (NoSuchBeanDefinitionException ignored) {
			String templateRef = element.getAttribute("mongo-template-ref");
			BeanDefinitionBuilder indexHelperBuilder = BeanDefinitionBuilder.genericBeanDefinition(MongoPersistentEntityIndexCreator.class);
			indexHelperBuilder.addConstructorArgValue(new RuntimeBeanReference(ctxRef));
			indexHelperBuilder.addConstructorArgValue(new RuntimeBeanReference(StringUtils.hasText(templateRef) ? templateRef : TEMPLATE));
			registry.registerBeanDefinition(INDEX_HELPER, indexHelperBuilder.getBeanDefinition());
		}

		return converterBuilder.getBeanDefinition();
	}


	public Set<String> getInititalEntityClasses(Element element, BeanDefinitionBuilder builder) {

		String basePackage = element.getAttribute(BASE_PACKAGE);

		if (!StringUtils.hasText(basePackage)) {
			return null;
		}

		ClassPathScanningCandidateComponentProvider componentProvider = new ClassPathScanningCandidateComponentProvider(false);
		componentProvider.addIncludeFilter(new AnnotationTypeFilter(Document.class));
		componentProvider.addIncludeFilter(new AnnotationTypeFilter(Persistent.class));

		Set<String> classes = new ManagedSet<String>();
		for (BeanDefinition candidate : componentProvider.findCandidateComponents(basePackage)) {
			classes.add(candidate.getBeanClassName());
		}

		return classes;
	}
}
