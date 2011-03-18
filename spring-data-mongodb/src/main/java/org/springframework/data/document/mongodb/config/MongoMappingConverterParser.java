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

import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.document.mongodb.convert.MappingMongoConverter;
import org.springframework.data.document.mongodb.mapping.Document;
import org.springframework.data.document.mongodb.mapping.MongoMappingConfigurationBuilder;
import org.springframework.data.document.mongodb.mapping.event.IndexCreationListener;
import org.springframework.data.mapping.BasicMappingContext;
import org.springframework.data.mapping.model.MappingException;
import org.w3c.dom.Element;

/**
 * Created by IntelliJ IDEA.
 * User: jbrisbin
 * Date: 2/28/11
 * Time: 9:26 AM
 * To change this template use File | Settings | File Templates.
 */
public class MongoMappingConverterParser extends AbstractBeanDefinitionParser {

  private static final String CONFIGURATION_BUILDER = "mappingConfigurationBuilder";
  private static final String MAPPING_CONTEXT = "mappingContext";
  private static final String INDEX_CREATION_LISTENER = "indexCreationListener";
  private static final String TEMPLATE = "mongoTemplate";
  private static final String BASE_PACKAGE = "base-package";

  @Override
  protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext) throws BeanDefinitionStoreException {
    return "mappingConverter";
  }

  @Override
  protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
    BeanDefinitionRegistry registry = parserContext.getRegistry();

    String builderRef = element.getAttribute("mapping-config-builder-ref");
    if (null == builderRef || "".equals(builderRef)) {
      GenericBeanDefinition builder = new GenericBeanDefinition();
      builder.setBeanClass(MongoMappingConfigurationBuilder.class);
      registry.registerBeanDefinition(CONFIGURATION_BUILDER, builder);
      builderRef = CONFIGURATION_BUILDER;
    }

    String ctxRef = element.getAttribute("mapping-context-ref");
    if (null == ctxRef || "".equals(ctxRef)) {
      BeanDefinitionBuilder mappingContextBuilder = BeanDefinitionBuilder.genericBeanDefinition(BasicMappingContext.class);
      mappingContextBuilder.addPropertyReference("mappingConfigurationBuilder", builderRef);
      registry.registerBeanDefinition(MAPPING_CONTEXT, mappingContextBuilder.getBeanDefinition());
      ctxRef = MAPPING_CONTEXT;
    }

    BeanDefinitionBuilder converterBuilder = BeanDefinitionBuilder.genericBeanDefinition(MappingMongoConverter.class);
    converterBuilder.addPropertyReference("mappingContext", ctxRef);

    String autowire = element.getAttribute("autowire");
    if (null != autowire || !"".equals(autowire)) {
      converterBuilder.addPropertyValue("autowirePersistentBeans", Boolean.parseBoolean(autowire));
    }

    // Need a reference to a MongoTemplate
    String mongoRef = element.getAttribute("mongo-ref");
    if (null == mongoRef || "".equals(mongoRef)) {
      mongoRef = "mongo";
    }
    converterBuilder.addPropertyReference("mongo", mongoRef);

    try {
      registry.getBeanDefinition(INDEX_CREATION_LISTENER);
    } catch (NoSuchBeanDefinitionException ignored) {
      String templateRef = element.getAttribute("mongo-template-ref");
      if (null == templateRef || "".equals(templateRef)) {
        templateRef = TEMPLATE;
      }
      BeanDefinitionBuilder indexListenerBuilder = BeanDefinitionBuilder.genericBeanDefinition(IndexCreationListener.class);
      indexListenerBuilder.addPropertyValue("mongoTemplate", new RuntimeBeanReference(templateRef));
      indexListenerBuilder.setDestroyMethodName("cleanUp");
      registry.registerBeanDefinition(INDEX_CREATION_LISTENER, indexListenerBuilder.getBeanDefinition());
    }

    // Scan for @Document entities
    String basePackage = element.getAttribute(BASE_PACKAGE);
    if (null != basePackage) {
      ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
      scanner.addIncludeFilter(new AnnotationTypeFilter(Document.class));
      scanner.addIncludeFilter(new AnnotationTypeFilter(Persistent.class));
      Set<BeanDefinition> entities = scanner.findCandidateComponents(basePackage);
      if (null != entities) {
        Set<Class<?>> initialEntitySet = new HashSet<Class<?>>(entities.size());
        for (BeanDefinition def : entities) {
          String clazzName = def.getBeanClassName();
          try {
            initialEntitySet.add(Class.forName(clazzName));
          } catch (ClassNotFoundException e) {
            throw new MappingException(e.getMessage(), e);
          }
        }
        converterBuilder.addPropertyValue("initialEntitySet", initialEntitySet);
      }
    }

    return converterBuilder.getBeanDefinition();
  }

}
