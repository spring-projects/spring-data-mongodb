/*
 * Copyright 2012 the original author or authors.
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

import org.junit.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.mapping.Document;

import com.mongodb.Mongo;

/**
 * Unit tests for {@link AbstractMongoConfiguration}.
 * 
 * @author Oliver Gierke
 */
public class AbstractMongoConfigurationUnitTests {

	/**
	 * @see DATAMONGO-496
	 */
	@Test
	public void usesConfigClassPackageAsBaseMappingPackage() throws ClassNotFoundException {

		AbstractMongoConfiguration configuration = new SampleMongoConfiguration();
		assertThat(configuration.getMappingBasePackage(), is(SampleMongoConfiguration.class.getPackage().getName()));
		assertThat(configuration.getInitialEntitySet(), hasSize(1));
		assertThat(configuration.getInitialEntitySet(), hasItem(Entity.class));
	}

	/**
	 * @see DATAMONGO-496
	 */
	@Test
	public void doesNotScanPackageIfMappingPackageIsNull() throws ClassNotFoundException {

		assertScanningDisabled(null);

	}

	/**
	 * @see DATAMONGO-496
	 */
	@Test
	public void doesNotScanPackageIfMappingPackageIsEmpty() throws ClassNotFoundException {

		assertScanningDisabled("");
		assertScanningDisabled(" ");
	}

	private static void assertScanningDisabled(final String value) throws ClassNotFoundException {

		AbstractMongoConfiguration configuration = new SampleMongoConfiguration() {
			@Override
			protected String getMappingBasePackage() {
				return value;
			}
		};

		assertThat(configuration.getMappingBasePackage(), is(value));
		assertThat(configuration.getInitialEntitySet(), hasSize(0));
	}

	static class SampleMongoConfiguration extends AbstractMongoConfiguration {

		@Override
		protected String getDatabaseName() {
			return "database";
		}

		@Bean
		@Override
		public Mongo mongo() throws Exception {
			return new Mongo();
		}
	}

	@Document
	static class Entity {

	}
}
