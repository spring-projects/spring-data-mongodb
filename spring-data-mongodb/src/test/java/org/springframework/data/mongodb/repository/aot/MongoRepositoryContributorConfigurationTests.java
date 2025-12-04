/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.mongodb.repository.aot;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.springframework.aot.generate.GeneratedFiles.Kind;
import org.springframework.aot.test.generate.TestGenerationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.aot.ApplicationContextAotGenerator;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.InputStreamSource;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

/**
 * @author Christoph Strobl
 */
class MongoRepositoryContributorConfigurationTests {

	@Configuration
	@EnableMongoRepositories(basePackages = "example.aot")
	static class ConfigurationWithoutAnythingSpecial {

	}

	@Configuration
	@EnableMongoRepositories(mongoTemplateRef = "template-2", basePackages = "example.aot")
	static class ConfigurationWithTemplateRef {

	}

	@Test // GH-5107
	void usesPrimaryMongoOperationsBeanReferenceByDefault() throws IOException {

		TestGenerationContext testContext = generate(ConfigurationWithoutAnythingSpecial.class);
		InputStreamSource file = testContext.getGeneratedFiles().getGeneratedFile(Kind.SOURCE,
				"example/aot/UserRepository__BeanDefinitions.java");

		InputStreamResource isr = new InputStreamResource(file);
		String sourceCode = isr.getContentAsString(StandardCharsets.UTF_8);

		assertThat(sourceCode).contains("operations = beanFactory.getBean(MongoOperations.class)");
	}

	@Test // GH-5107
	void shouldConsiderMongoTemplateReferenceIfPresent() throws IOException {

		TestGenerationContext testContext = generate(ConfigurationWithTemplateRef.class);
		InputStreamSource file = testContext.getGeneratedFiles().getGeneratedFile(Kind.SOURCE,
				"example/aot/UserRepository__BeanDefinitions.java");

		InputStreamResource isr = new InputStreamResource(file);
		String sourceCode = isr.getContentAsString(StandardCharsets.UTF_8);

		assertThat(sourceCode).contains("operations = beanFactory.getBean(\"template-2\", MongoOperations.class)");
	}

	private static TestGenerationContext generate(Class<?>... configurationClasses) {

		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.register(configurationClasses);

		ApplicationContextAotGenerator generator = new ApplicationContextAotGenerator();

		TestGenerationContext generationContext = new TestGenerationContext();
		generator.processAheadOfTime(context, generationContext);
		generationContext.writeGeneratedContent();
		return generationContext;
	}
}
