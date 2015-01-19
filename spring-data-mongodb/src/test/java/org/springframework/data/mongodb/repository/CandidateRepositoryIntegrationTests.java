/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Integration tests for {@link CandidateRepository}.
 *
 * @author Mateusz Rasiñski
 */
public class CandidateRepositoryIntegrationTests extends PersonRepositoryIntegrationTests {

	@Autowired protected CandidateRepository candidateRepository;

	/**
	 * @see DATAMONGO-1142
	 */
	@Test
	public void personRepositoryCanSaveAndFindCandidate() {
		Candidate candidate = repository.save(new Candidate("Jan", "Kowalski", "Mr."));

		Person result = repository.findOne(candidate.getId());

		assertThat(result, notNullValue());
		assertThat(result, equalTo((Person) candidate));
		assertThat(result, instanceOf(Candidate.class));
		assertThat(((Candidate) result).getTitle(), is("Mr."));
	}

	/**
	 * @see DATAMONGO-1142
	 */
	@Test
	public void personRepositoryReturnsAlsoCandidates() {
		Candidate candidate = candidateRepository.save(new Candidate("Jan", "Kowalski"));

		List<Person> persons = repository.findAll();

		assertThat(persons, hasSize(greaterThan(1)));
		assertThat(persons.contains(candidate), is(true));
	}

	/**
	 * @see DATAMONGO-1142
	 */
	@Test
	public void candidateRepositoryReturnsOnlyCandidates() {
		Candidate candidate = candidateRepository.save(new Candidate("Jan", "Kowalski"));

		List<Candidate> candidates = candidateRepository.findAll();

		assertThat(candidates, hasSize(1));
		assertThat(candidates, contains(candidate));
	}
}
