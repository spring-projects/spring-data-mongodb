/*
 * Copyright 2013-2018 the original author or authors.
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
package org.springframework.data.mongodb.config;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.config.BeanComponentDefinitionBuilder;
import org.springframework.data.mongodb.gridfs.GridFsTemplate;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * {@link BeanDefinitionParser} to parse {@code gridFsTemplate} elements into {@link BeanDefinition}s.
 *
 * @author Martin Baumgartner
 */
class GridFsTemplateParser extends AbstractBeanDefinitionParser {

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.AbstractBeanDefinitionParser#resolveId(org.w3c.dom.Element, org.springframework.beans.factory.support.AbstractBeanDefinition, org.springframework.beans.factory.xml.ParserContext)
	 */
	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);
		return StringUtils.hasText(id) ? id : BeanNames.GRID_FS_TEMPLATE_BEAN_NAME;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.AbstractBeanDefinitionParser#parseInternal(org.w3c.dom.Element, org.springframework.beans.factory.xml.ParserContext)
	 */
	@Override
	protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {

		BeanComponentDefinitionBuilder helper = new BeanComponentDefinitionBuilder(element, parserContext);

		String converterRef = element.getAttribute("converter-ref");
		String dbFactoryRef = element.getAttribute("db-factory-ref");
		String bucket = element.getAttribute("bucket");

		BeanDefinitionBuilder gridFsTemplateBuilder = BeanDefinitionBuilder.genericBeanDefinition(GridFsTemplate.class);

		if (StringUtils.hasText(dbFactoryRef)) {
			gridFsTemplateBuilder.addConstructorArgReference(dbFactoryRef);
		} else {
			gridFsTemplateBuilder.addConstructorArgReference(BeanNames.DB_FACTORY_BEAN_NAME);
		}

		if (StringUtils.hasText(converterRef)) {
			gridFsTemplateBuilder.addConstructorArgReference(converterRef);
		} else {
			gridFsTemplateBuilder.addConstructorArgReference(BeanNames.DEFAULT_CONVERTER_BEAN_NAME);
		}

		if (StringUtils.hasText(bucket)) {
			gridFsTemplateBuilder.addConstructorArgValue(bucket);
		}

		return (AbstractBeanDefinition) helper.getComponentIdButFallback(gridFsTemplateBuilder, BeanNames.GRID_FS_TEMPLATE_BEAN_NAME)
				.getBeanDefinition();
	}
}
