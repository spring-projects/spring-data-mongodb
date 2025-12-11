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

import org.springframework.aot.generate.GeneratedFiles;
import org.springframework.aot.test.generate.TestGenerationContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.InputStreamSource;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.repository.Meta;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.data.repository.CrudRepository;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Unit tests for the {@link UserRepository} fragment sources via {@link MongoRepositoryContributor}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@SpringJUnitConfig(classes = MongoRepositoryContributorUnitTests.MongoRepositoryContributorConfiguration.class)
class MongoRepositoryContributorUnitTests {

	@Configuration
	@EnableMongoRepositories(considerNestedRepositories = true, mongoTemplateRef = "mongoOperations",
			includeFilters = { @ComponentScan.Filter(classes = MetaUserRepository.class, type = FilterType.ASSIGNABLE_TYPE) })
	static class MongoRepositoryContributorConfiguration extends AotFragmentTestConfigurationSupport {

		public MongoRepositoryContributorConfiguration() {
			super(MetaUserRepository.class, MongoRepositoryContributorConfiguration.class);
		}

		@Bean
		MongoOperations mongoOperations(MongoConverter mongoConverter) {
			MongoOperations operations = mock(MongoOperations.class);
			when(operations.getConverter()).thenReturn(mongoConverter);
			return operations;
		}

	}

	@Autowired TestGenerationContext generationContext;

	@Test // GH-4970, GH-4667
	void shouldConsiderMetaAnnotation() throws IOException {

		InputStreamSource aotFragment = generationContext.getGeneratedFiles().getGeneratedFile(GeneratedFiles.Kind.SOURCE,
				AotFragmentTestConfigurationSupport.getAotImplFragmentName(MetaUserRepository.class).replace('.', '/')
						+ ".java");

		String content = new InputStreamResource(aotFragment).getContentAsString(StandardCharsets.UTF_8);

		assertThat(content).contains("filterQuery.maxTimeMsec(555)");
		assertThat(content).contains("filterQuery.cursorBatchSize(1234)");
		assertThat(content).contains("filterQuery.comment(\"foo\")");
		assertThat(content).contains("filterQuery.diskUse(DiskUse.DENY)");
	}

	public interface MetaUserRepository extends CrudRepository<User, String> {

		@Meta
		User findAllByLastname(String lastname);

		@Meta(cursorBatchSize = 1234, comment = "foo", maxExecutionTimeMs = 555, allowDiskUse = "false")
		User findWithMetaAllByLastname(String lastname);
	}

}
