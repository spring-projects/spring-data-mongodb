/*
 * Copyright 2011-2014 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Collections;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mongodb.core.convert.CustomConversions;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoTypeMapper;
import org.springframework.data.mongodb.core.mapping.Account;
import org.springframework.data.mongodb.core.mapping.CamelCaseAbbreviatingFieldNamingStrategy;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.stereotype.Component;

import com.mongodb.DBObject;

/**
 * Integration tests for {@link MappingMongoConverterParser}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
public class MappingMongoConverterParserIntegrationTests {

	DefaultListableBeanFactory factory;

	@Before
	public void setUp() {
		factory = new DefaultListableBeanFactory();
		XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(factory);
		reader.loadBeanDefinitions(new ClassPathResource("namespace/converter.xml"));
	}

	/**
	 * @see DATAMONGO-243
	 */
	@Test
	public void allowsDbFactoryRefAttribute() {

		factory.getBeanDefinition("converter");
		factory.getBean("converter");
	}

	/**
	 * @see DATAMONGO-725
	 */
	@Test
	public void hasCustomTypeMapper() {

		MappingMongoConverter converter = factory.getBean("converter", MappingMongoConverter.class);
		MongoTypeMapper customMongoTypeMapper = factory.getBean(CustomMongoTypeMapper.class);

		assertThat(converter.getTypeMapper(), is(customMongoTypeMapper));
	}

	/**
	 * @see DATAMONGO-301
	 */
	@Test
	public void scansForConverterAndSetsUpCustomConversionsAccordingly() {

		CustomConversions conversions = factory.getBean(CustomConversions.class);
		assertThat(conversions.hasCustomWriteTarget(Person.class), is(true));
		assertThat(conversions.hasCustomWriteTarget(Account.class), is(true));
	}

	/**
	 * @see DATAMONGO-607
	 */
	@Test
	public void activatesAbbreviatingPropertiesCorrectly() {

		BeanDefinition definition = factory.getBeanDefinition("abbreviatingConverter.mongoMappingContext");
		Object value = definition.getPropertyValues().getPropertyValue("fieldNamingStrategy").getValue();

		assertThat(value, is(instanceOf(BeanDefinition.class)));
		BeanDefinition strategy = (BeanDefinition) value;
		assertThat(strategy.getBeanClassName(), is(CamelCaseAbbreviatingFieldNamingStrategy.class.getName()));
	}

	@Component
	public static class SampleConverter implements Converter<Person, DBObject> {
		public DBObject convert(Person source) {
			return null;
		}
	}

	@Component
	public static class SampleConverterFactory implements GenericConverter {

		public Set<ConvertiblePair> getConvertibleTypes() {
			return Collections.singleton(new ConvertiblePair(Account.class, DBObject.class));
		}

		public Object convert(Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {
			return null;
		}
	}
}
