package org.springframework.data.mongodb.config;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.parsing.BeanComponentDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveValidator;
import org.springframework.scheduling.config.AnnotationDrivenBeanDefinitionParser;
import org.springframework.util.ClassUtils;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import org.w3c.dom.Element;

public class MongoValidationParser extends AbstractBeanDefinitionParser {
	private static final boolean jsr303Present = ClassUtils.isPresent("javax.validation.Validator", AnnotationDrivenBeanDefinitionParser.class.getClassLoader());

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext) throws BeanDefinitionStoreException {
		return "beforeSaveValidator";
	}

	@Override
	protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition();

		RuntimeBeanReference validator = getValidator(builder, parserContext);

		if (validator == null) {
			return null;
		} else {
			builder.getRawBeanDefinition().setBeanClass(BeforeSaveValidator.class);
			builder.addPropertyValue("validator", validator);

			return builder.getBeanDefinition();
		}
	}

	private RuntimeBeanReference getValidator(Object source, ParserContext parserContext) {
		if (!jsr303Present) {
			return null;
		}

		RootBeanDefinition validatorDef = new RootBeanDefinition(LocalValidatorFactoryBean.class);
		validatorDef.setSource(source);
		validatorDef.setRole(BeanDefinition.ROLE_INFRASTRUCTURE);
		String validatorName = parserContext.getReaderContext().registerWithGeneratedName(validatorDef);
		parserContext.registerComponent(new BeanComponentDefinition(validatorDef, validatorName));

		return new RuntimeBeanReference(validatorName);
	}
}
