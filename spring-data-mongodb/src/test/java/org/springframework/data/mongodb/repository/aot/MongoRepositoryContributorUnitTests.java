/*
 * Copyright 2025 the original author or authors.
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

import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import example.aot.User;
import example.aot.UserRepository;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.aot.generate.GeneratedFiles;
import org.springframework.aot.test.generate.TestGenerationContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.InputStreamSource;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.repository.Meta;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.repository.CrudRepository;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Unit tests for the {@link UserRepository} fragment sources via {@link MongoRepositoryContributor}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MongoClientExtension.class)
@SpringJUnitConfig(classes = MongoRepositoryContributorUnitTests.MongoRepositoryContributorConfiguration.class)
class MongoRepositoryContributorUnitTests {

	@Configuration
	static class MongoRepositoryContributorConfiguration extends AotFragmentTestConfigurationSupport {

		public MongoRepositoryContributorConfiguration() {
			super(MetaUserRepository.class);
		}

		@Bean
		MongoOperations mongoOperations() {
			return mock(MongoOperations.class);
		}

	}

	@Autowired TestGenerationContext generationContext;

	@Test
	void shouldConsiderMetaAnnotation() throws IOException {

		InputStreamSource aotFragment = generationContext.getGeneratedFiles().getGeneratedFile(GeneratedFiles.Kind.SOURCE,
				MetaUserRepository.class.getPackageName().replace('.', '/') + "/MetaUserRepositoryImpl__Aot.java");

		String content = new InputStreamResource(aotFragment).getContentAsString(StandardCharsets.UTF_8);

		assertThat(content).contains("filterQuery.maxTimeMsec(555)");
		assertThat(content).contains("filterQuery.cursorBatchSize(1234)");
		assertThat(content).contains("filterQuery.comment(\"foo\")");
	}

	interface MetaUserRepository extends CrudRepository<User, String> {

		@Meta
		User findAllByLastname(String lastname);

		@Meta(cursorBatchSize = 1234, comment = "foo", maxExecutionTimeMs = 555)
		User findWithMetaAllByLastname(String lastname);
	}

}
