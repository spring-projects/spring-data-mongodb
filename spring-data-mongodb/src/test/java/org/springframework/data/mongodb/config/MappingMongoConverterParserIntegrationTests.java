/*
 * Copyright 2011-2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.util.Collections;
import java.util.Set;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanReference;
import org.springframework.beans.factory.parsing.BeanDefinitionParsingException;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.model.CamelCaseAbbreviatingFieldNamingStrategy;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoTypeMapper;
import org.springframework.data.mongodb.core.mapping.Account;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.stereotype.Component;

/**
 * Integration tests for {@link MappingMongoConverterParser}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Ryan Tenney
 */
public class MappingMongoConverterParserIntegrationTests {

	private DefaultListableBeanFactory factory;

	@Test // DATAMONGO-243
	void allowsDbFactoryRefAttribute() {

		loadValidConfiguration();
		factory.getBeanDefinition("converter");
		factory.getBean("converter");
	}

	@Test // DATAMONGO-725
	void hasCustomTypeMapper() {

		loadValidConfiguration();
		MappingMongoConverter converter = factory.getBean("converter", MappingMongoConverter.class);
		MongoTypeMapper customMongoTypeMapper = factory.getBean(CustomMongoTypeMapper.class);

		assertThat(converter.getTypeMapper()).isEqualTo(customMongoTypeMapper);
	}

	@Test // DATAMONGO-301
	void scansForConverterAndSetsUpCustomConversionsAccordingly() {

		loadValidConfiguration();
		CustomConversions conversions = factory.getBean(CustomConversions.class);
		assertThat(conversions.hasCustomWriteTarget(Person.class)).isTrue();
		assertThat(conversions.hasCustomWriteTarget(Account.class)).isTrue();
	}

	@Test // DATAMONGO-607
	void activatesAbbreviatingPropertiesCorrectly() {

		loadValidConfiguration();
		BeanDefinition definition = factory.getBeanDefinition("abbreviatingConverter.mongoMappingContext");
		Object value = definition.getPropertyValues().getPropertyValue("fieldNamingStrategy").getValue();

		assertThat(value).isInstanceOf(BeanDefinition.class);
		BeanDefinition strategy = (BeanDefinition) value;
		assertThat(strategy.getBeanClassName()).isEqualTo(CamelCaseAbbreviatingFieldNamingStrategy.class.getName());
	}

	@Test // DATAMONGO-866
	void rejectsInvalidFieldNamingStrategyConfiguration() {

		BeanDefinitionRegistry factory = new DefaultListableBeanFactory();
		XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(factory);

		assertThatExceptionOfType(BeanDefinitionParsingException.class)
				.isThrownBy(() -> reader.loadBeanDefinitions(new ClassPathResource("namespace/converter-invalid.xml")))
				.withMessageContaining("abbreviation").withMessageContaining("field-naming-strategy-ref");
	}

	@Test // DATAMONGO-892
	void shouldThrowBeanDefinitionParsingExceptionIfConverterDefinedAsNestedBean() {
		assertThatExceptionOfType(BeanDefinitionParsingException.class).isThrownBy(this::loadNestedBeanConfiguration);
	}

	@Test // DATAMONGO-925, DATAMONGO-928
	void shouldSupportCustomFieldNamingStrategy() {
		assertStrategyReferenceSetFor("mappingConverterWithCustomFieldNamingStrategy");
	}

	@Test // DATAMONGO-925, DATAMONGO-928
	void shouldNotFailLoadingConfigIfAbbreviationIsDisabledAndStrategySet() {
		assertStrategyReferenceSetFor("mappingConverterWithCustomFieldNamingStrategyAndAbbreviationDisabled");
	}

	private void loadValidConfiguration() {
		this.loadConfiguration("namespace/converter.xml");
	}

	private void loadNestedBeanConfiguration() {
		this.loadConfiguration("namespace/converter-nested-bean-definition.xml");
	}

	private void loadConfiguration(String configLocation) {
		factory = new DefaultListableBeanFactory();
		factory.setAllowBeanDefinitionOverriding(false);
		XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(factory);
		reader.loadBeanDefinitions(new ClassPathResource(configLocation));
	}

	private static void assertStrategyReferenceSetFor(String beanId) {

		DefaultListableBeanFactory factory = new DefaultListableBeanFactory();
		factory.setAllowBeanDefinitionOverriding(false);
		XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(factory);
		reader.loadBeanDefinitions(new ClassPathResource("namespace/converter-custom-fieldnamingstrategy.xml"));

		BeanDefinition definition = reader.getRegistry().getBeanDefinition(beanId.concat(".mongoMappingContext"));
		BeanReference value = (BeanReference) definition.getPropertyValues().getPropertyValue("fieldNamingStrategy")
				.getValue();

		assertThat(value.getBeanName()).isEqualTo("customFieldNamingStrategy");
	}

	@Component
	public static class SampleConverter implements Converter<Person, Document> {
		public Document convert(Person source) {
			return null;
		}
	}

	@Component
	public static class SampleConverterFactory implements GenericConverter {

		public Set<ConvertiblePair> getConvertibleTypes() {
			return Collections.singleton(new ConvertiblePair(Account.class, Document.class));
		}

		public Object convert(Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {
			return null;
		}
	}
}
