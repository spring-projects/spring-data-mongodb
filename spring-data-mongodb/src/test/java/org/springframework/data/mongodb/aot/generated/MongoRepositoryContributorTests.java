/*
 * Copyright 2025. the original author or authors.
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

/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.aot.generated;

import static org.assertj.core.api.Assertions.assertThat;

import example.aot.User;
import example.aot.UserRepository;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.aot.test.generate.TestGenerationContext;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.core.test.tools.TestCompiler;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.data.util.Lazy;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.client.MongoClient;

/**
 * @author Christoph Strobl
 * @since 2025/01
 */
@ExtendWith(MongoClientExtension.class)
public class MongoRepositoryContributorTests {

	private static final String DB_NAME = "aot-repo-tests";
	private static Verifyer generated;

	@Client static MongoClient client;

	@BeforeAll
	static void beforeAll() {

		TestMongoAotRepositoryContext aotContext = new TestMongoAotRepositoryContext(UserRepository.class, null);
		TestGenerationContext generationContext = new TestGenerationContext(UserRepository.class);

		new MongoRepositoryContributor(aotContext).contribute(generationContext);

		AbstractBeanDefinition mongoTemplate = BeanDefinitionBuilder.rootBeanDefinition(MongoTestTemplate.class)
				.addConstructorArgValue(DB_NAME).getBeanDefinition();
		AbstractBeanDefinition aotGeneratedRepository = BeanDefinitionBuilder
				.genericBeanDefinition("example.aot.UserRepositoryImpl__Aot").addConstructorArgReference("mongoOperations")
				.getBeanDefinition();

		generated = generateContext(generationContext) //
				.register("mongoOperations", mongoTemplate) //
				.register("aotUserRepository", aotGeneratedRepository);
	}

	@BeforeEach
	void beforeEach() {

		MongoTestUtils.flushCollection(DB_NAME, "user", client);
		initUsers();
	}

	@Test
	void testFindDerivedFinderSingleEntity() {

		generated.verify(methodInvoker -> {

			User user = methodInvoker.invoke("findOneByUsername", "yoda").onBean("aotUserRepository");
			assertThat(user).isNotNull().extracting(User::getUsername).isEqualTo("yoda");
		});
	}

	@Test
	void testFindDerivedFinderOptionalEntity() {

		generated.verify(methodInvoker -> {

			Optional<User> user = methodInvoker.invoke("findOptionalOneByUsername", "yoda").onBean("aotUserRepository");
			assertThat(user).isNotNull().containsInstanceOf(User.class)
					.hasValueSatisfying(it -> assertThat(it).extracting(User::getUsername).isEqualTo("yoda"));
		});
	}

	@Test
	void testDerivedCount() {

		generated.verify(methodInvoker -> {

			Long value = methodInvoker.invoke("countUsersByLastname", "Skywalker").onBean("aotUserRepository");
			assertThat(value).isEqualTo(2L);
		});
	}

	@Test
	void testDerivedExists() {

		generated.verify(methodInvoker -> {

			Boolean exists = methodInvoker.invoke("existsUserByLastname", "Skywalker").onBean("aotUserRepository");
			assertThat(exists).isTrue();
		});
	}

	@Test
	void testDerivedFinderWithoutArguments() {

		generated.verify(methodInvoker -> {

			List<User> users = methodInvoker.invoke("findUserNoArgumentsBy").onBean("aotUserRepository");
			assertThat(users).hasSize(7).hasOnlyElementsOfType(User.class);
		});
	}

	@Test
	void testCountWorksAsExpected() {

		generated.verify(methodInvoker -> {

			Long value = methodInvoker.invoke("countUsersByLastname", "Skywalker").onBean("aotUserRepository");
			assertThat(value).isEqualTo(2L);
		});
	}

	@Test
	void testLimitedDerivedFinder() {

		generated.verify(methodInvoker -> {

			List<User> users = methodInvoker.invoke("findTop2ByLastnameStartingWith", "S").onBean("aotUserRepository");
			assertThat(users).hasSize(2);
		});
	}

	// countUsersByLastname

	private static void initUsers() {

		Document luke = Document.parse("""
				{
				  "_id": "id-1",
				  "username": "luke",
				  "first_name": "Luke",
				  "last_name": "Skywalker",
				  "posts": [
				    {
				      "message": "I have a bad feeling about this.",
				      "date": {
				        "$date": "2025-01-15T12:50:33.855Z"
				      }
				    }
				  ],
				  "_class": "example.springdata.aot.User"
				}""");

		Document leia = Document.parse("""
				{
				  "_id": "id-2",
				  "username": "leia",
				  "first_name": "Leia",
				  "last_name": "Organa",
				  "_class": "example.springdata.aot.User"
				}""");

		Document han = Document.parse("""
				{
				  "_id": "id-3",
				  "username": "han",
				  "first_name": "Han",
				  "last_name": "Solo",
				  "posts": [
				    {
				      "message": "It's the ship that made the Kessel Run in less than 12 Parsecs.",
				      "date": {
				        "$date": "2025-01-15T13:30:33.855Z"
				      }
				    }
				  ],
				  "_class": "example.springdata.aot.User"
				}""");

		Document chwebacca = Document.parse("""
				{
				  "_id": "id-4",
				  "username": "chewbacca",
				  "_class": "example.springdata.aot.User"
				}""");

		Document yoda = Document.parse(
				"""
						{
						  "_id": "id-5",
						  "username": "yoda",
						  "posts": [
						    {
						      "message": "Do. Or do not. There is no try.",
						      "date": {
						        "$date": "2025-01-15T13:09:33.855Z"
						      }
						    },
						    {
						      "message": "Decide you must, how to serve them best. If you leave now, help them you could; but you would destroy all for which they have fought, and suffered.",
						      "date": {
						        "$date": "2025-01-15T13:53:33.855Z"
						      }
						    }
						  ]
						}""");

		Document vader = Document.parse("""
				{
				  "_id": "id-6",
				  "username": "vader",
				  "first_name": "Anakin",
				  "last_name": "Skywalker",
				  "posts": [
				    {
				      "message": "I am your father",
				      "date": {
				        "$date": "2025-01-15T13:46:33.855Z"
				      }
				    }
				  ]
				}""");

		Document kylo = Document.parse("""
				{
				  "_id": "id-7",
				  "username": "kylo",
				  "first_name": "Ben",
				  "last_name": "Solo"
				}
				""");

		client.getDatabase(DB_NAME).getCollection("user")
				.insertMany(List.of(luke, leia, han, chwebacca, yoda, vader, kylo));
	}

	static GeneratedContextBuilder generateContext(TestGenerationContext generationContext) {
		return new GeneratedContextBuilder(generationContext);
	}

	static class GeneratedContextBuilder implements Verifyer {

		TestGenerationContext generationContext;
		Map<String, BeanDefinition> beanDefinitions = new LinkedHashMap<>();
		Lazy<DefaultListableBeanFactory> lazyFactory;

		public GeneratedContextBuilder(TestGenerationContext generationContext) {

			this.generationContext = generationContext;
			this.lazyFactory = Lazy.of(() -> {
				DefaultListableBeanFactory freshBeanFactory = new DefaultListableBeanFactory();
				TestCompiler.forSystem().with(generationContext).compile(compiled -> {

					freshBeanFactory.setBeanClassLoader(compiled.getClassLoader());
					for (Entry<String, BeanDefinition> entry : beanDefinitions.entrySet()) {
						freshBeanFactory.registerBeanDefinition(entry.getKey(), entry.getValue());
					}
				});
				return freshBeanFactory;
			});
		}

		GeneratedContextBuilder register(String name, BeanDefinition beanDefinition) {
			this.beanDefinitions.put(name, beanDefinition);
			return this;
		}

		public Verifyer verify(Consumer<GeneratedContext> methodInvoker) {
			methodInvoker.accept(new GeneratedContext(lazyFactory));
			return this;
		}

	}

	interface Verifyer {
		Verifyer verify(Consumer<GeneratedContext> methodInvoker);
	}

	static class GeneratedContext {

		private Supplier<DefaultListableBeanFactory> delegate;

		public GeneratedContext(Supplier<DefaultListableBeanFactory> defaultListableBeanFactory) {
			this.delegate = defaultListableBeanFactory;
		}

		InvocationBuilder invoke(String method, Object... arguments) {

			return new InvocationBuilder() {
				@Override
				public <T> T onBean(String beanName) {
					Object bean = delegate.get().getBean(beanName);
					return ReflectionTestUtils.invokeMethod(bean, method, arguments);
				}
			};
		}

		interface InvocationBuilder {
			<T> T onBean(String beanName);
		}

	}
}
