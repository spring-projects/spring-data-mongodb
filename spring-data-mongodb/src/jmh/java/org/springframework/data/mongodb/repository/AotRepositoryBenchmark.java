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
package org.springframework.data.mongodb.repository;

import org.junit.platform.commons.annotation.Testable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import org.springframework.aot.test.generate.TestGenerationContext;
import org.springframework.core.test.tools.TestCompiler;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.microbenchmark.AbstractMicrobenchmark;
import org.springframework.data.mongodb.repository.aot.MongoRepositoryContributor;
import org.springframework.data.mongodb.repository.aot.TestMongoAotRepositoryContext;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactory;
import org.springframework.data.mongodb.repository.support.QuerydslMongoPredicateExecutor;
import org.springframework.data.mongodb.repository.support.SimpleMongoRepository;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryComposition;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFragment;
import org.springframework.data.repository.query.ValueExpressionDelegate;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

/**
 * Benchmark for AOT repositories.
 *
 * @author Mark Paluch
 */
@Testable
public class AotRepositoryBenchmark extends AbstractMicrobenchmark {

	@State(Scope.Benchmark)
	public static class BenchmarkParameters {

		public static Class<?> aot;
		public static TestMongoAotRepositoryContext repositoryContext = new TestMongoAotRepositoryContext(
				SmallerPersonRepository.class,
				RepositoryComposition.of(RepositoryFragment.structural(SimpleMongoRepository.class),
						RepositoryFragment.structural(QuerydslMongoPredicateExecutor.class)));

		MongoClient mongoClient;
		MongoTemplate mongoTemplate;
		RepositoryComposition.RepositoryFragments fragments;
		SmallerPersonRepository repositoryProxy;

		@Setup(Level.Trial)
		public void doSetup() {

			mongoClient = MongoClients.create();
			mongoTemplate = new MongoTemplate(mongoClient, "jmh");

			if (this.aot == null) {

				TestGenerationContext generationContext = new TestGenerationContext(PersonRepository.class);

				new MongoRepositoryContributor(repositoryContext).contribute(generationContext);

				TestCompiler.forSystem().withCompilerOptions("-parameters").with(generationContext).compile(compiled -> {

					try {
						this.aot = compiled.getClassLoader().loadClass(SmallerPersonRepository.class.getName() + "Impl__Aot");
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				});
			}

			try {
				RepositoryFactoryBeanSupport.FragmentCreationContext creationContext = getCreationContext(repositoryContext);
				fragments = RepositoryComposition.RepositoryFragments
						.just(aot.getConstructor(MongoOperations.class, RepositoryFactoryBeanSupport.FragmentCreationContext.class)
								.newInstance(mongoTemplate, creationContext));

				this.repositoryProxy = createRepository(fragments);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		private RepositoryFactoryBeanSupport.FragmentCreationContext getCreationContext(
				TestMongoAotRepositoryContext repositoryContext) {

			RepositoryFactoryBeanSupport.FragmentCreationContext creationContext = new RepositoryFactoryBeanSupport.FragmentCreationContext() {
				@Override
				public RepositoryMetadata getRepositoryMetadata() {
					return repositoryContext.getRepositoryInformation();
				}

				@Override
				public ValueExpressionDelegate getValueExpressionDelegate() {
					return ValueExpressionDelegate.create();
				}

				@Override
				public ProjectionFactory getProjectionFactory() {
					return new SpelAwareProxyProjectionFactory();
				}
			};

			return creationContext;
		}

		@TearDown(Level.Trial)
		public void doTearDown() {
			mongoClient.close();
		}

		public SmallerPersonRepository createRepository(RepositoryComposition.RepositoryFragments fragments) {
			MongoRepositoryFactory repositoryFactory = new MongoRepositoryFactory(mongoTemplate);
			return repositoryFactory.getRepository(SmallerPersonRepository.class, fragments);
		}

	}

	@Benchmark
	public SmallerPersonRepository repositoryBootstrap(BenchmarkParameters parameters) {
		return parameters.createRepository(parameters.fragments);
	}

	@Benchmark
	public Object findDerived(BenchmarkParameters parameters) {
		return parameters.repositoryProxy.findByFirstname("foo");
	}

	@Benchmark
	public Object findAnnotated(BenchmarkParameters parameters) {
		return parameters.repositoryProxy.findByThePersonsFirstname("foo");
	}

}
