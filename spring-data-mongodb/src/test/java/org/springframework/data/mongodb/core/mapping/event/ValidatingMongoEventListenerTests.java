/*
 * Copyright 2012-2016 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping.event;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.UUID;

import javax.validation.ConstraintViolationException;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.convert.LazyLoadingProxy;
import org.springframework.data.mongodb.core.mapping.event.User.AddressRelation;
import org.springframework.data.mongodb.core.mapping.event.repo.AddressRepository;
import org.springframework.data.mongodb.core.mapping.event.repo.UserRepository;
import org.springframework.data.mongodb.test.util.MongoVersionRule;
import org.springframework.data.util.Version;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration test for {@link ValidatingMongoEventListener}.
 * 
 * @see DATAMONGO-36
 * @author Maciej Walkowiak
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Paul Sterl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class ValidatingMongoEventListenerTests {

	public static @ClassRule MongoVersionRule version = MongoVersionRule.atLeast(new Version(2, 6));

	@Autowired AddressRepository addressRepo;
	@Autowired UserRepository userRepo;

	@Test
	public void shouldThrowConstraintViolationException() {

		User user = new User("john", 17);

		try {
			userRepo.save(user);
			fail();
		} catch (ConstraintViolationException e) {
			assertThat(e.getConstraintViolations().size(), equalTo(2));
		}
	}

	@Test
	public void shouldNotThrowAnyExceptions() {
		userRepo.save(new User("john smith", 18));
	}
	
	/**
	 * @see DATAMONGO-1393
	 */
	@Test
	public void validationOfLazyProxy() {
		Address address = addressRepo.insert(new Address(UUID.randomUUID().toString()));
		User user = userRepo.insert(new User("Foo Bar Name 123456", 18, address));
		
		user = userRepo.findOne(user.getId());
		assertTrue("Spring should use now a LazyLoadingProxy.", user.getAddresses().get(0).getAddress() instanceof LazyLoadingProxy);
		address = user.getAddresses().get(0).getAddress();
		address.setName("New Fancy Address Name");
		addressRepo.save(address);
	}
	/**
	 * @see DATAMONGO-1393
	 */
	@Test(expected = ConstraintViolationException.class)
	public void validationErrorTest() {
		Address address = addressRepo.insert(new Address(UUID.randomUUID().toString()));
		User user = userRepo.insert(new User("Foo Bar Name 123456", 18, address));
		
		user = userRepo.findOne(user.getId());
		address = user.getAddresses().get(0).getAddress();
		address.setName(null);
		addressRepo.save(address);
	}
}
