package org.springframework.data.mongodb.repository;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.ContextConfiguration;

/**
 * @author Thomas Darimont
 */
@ContextConfiguration("config/MongoNamespaceIntegrationTests-context.xml")
public class RedeclaringRepositoryMethodsTests extends AbstractPersonRepositoryIntegrationTests {

	@Autowired RedeclaringRepositoryMethodsRepository repository;

	/**
	 * @see DATAMONGO-760
	 */
	@Test
	public void adjustedWellKnownPagedFindAllMethodShouldReturnOnlyTheUserWithFirstnameOliverAugust() {

		Page<Person> page = repository.findAll(new PageRequest(0, 2));

		assertThat(page.getNumberOfElements(), is(1));
		assertThat(page.getContent().get(0).getFirstname(), is(oliver.getFirstname()));
	}

	/**
	 * @see DATAMONGO-760
	 */
	@Test
	public void adjustedWllKnownFindAllMethodShouldReturnAnEmptyList() {

		List<Person> result = repository.findAll();

		assertThat(result.isEmpty(), is(true));
	}

}
