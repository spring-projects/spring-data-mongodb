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

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.microbenchmark.AbstractMicrobenchmark;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

/**
 * Benchmark for AOT repositories.
 *
 * @author Mark Paluch
 */
@Testable
public class SmallerRepositoryBenchmark extends AbstractMicrobenchmark {

	@State(Scope.Benchmark)
	public static class BenchmarkParameters {

		MongoClient mongoClient;
		MongoTemplate mongoTemplate;
		SmallerPersonRepository repositoryProxy;

		@Setup(Level.Trial)
		public void doSetup() {

			mongoClient = MongoClients.create();
			mongoTemplate = new MongoTemplate(mongoClient, "jmh");
			repositoryProxy = createRepository();
		}

		@TearDown(Level.Trial)
		public void doTearDown() {
			mongoClient.close();
		}

		public SmallerPersonRepository createRepository() {
			MongoRepositoryFactory repositoryFactory = new MongoRepositoryFactory(mongoTemplate);
			return repositoryFactory.getRepository(SmallerPersonRepository.class);
		}

	}

	@Benchmark
	public SmallerPersonRepository repositoryBootstrap(BenchmarkParameters parameters) {
		return parameters.createRepository();
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
