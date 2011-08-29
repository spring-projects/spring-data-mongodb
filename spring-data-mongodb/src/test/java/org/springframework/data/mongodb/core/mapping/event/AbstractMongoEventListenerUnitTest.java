/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.mapping.event;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.mongodb.repository.Person;

/**
 * Unit tests for {@link AbstractMongoEventListener}.
 *
 * @author Oliver Gierke
 */
public class AbstractMongoEventListenerUnitTest {
	
	@Test
	public void invokesCallbackForEventForPerson() {
		
		MongoMappingEvent<Person> event = new BeforeConvertEvent<Person>(new Person("Dave", "Matthews"));
		SampleEventListener listener = new SampleEventListener();
		listener.onApplicationEvent(event);
		assertThat(listener.invoked, is(true));
	}
	
	@Test
	public void dropsEventIfNotForCorrectDomainType() {
		
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext();
		context.refresh();
		
		SampleEventListener listener = new SampleEventListener();
		context.addApplicationListener(listener);
		
		context.publishEvent(new BeforeConvertEvent<Person>(new Person("Dave", "Matthews")));
		assertThat(listener.invoked, is(true));
		
		listener.invoked = false;
		context.publishEvent(new BeforeConvertEvent<String>("Test"));
		assertThat(listener.invoked, is(false));
		
	}

	class SampleEventListener extends AbstractMongoEventListener<Person> {
		
		boolean invoked;
		
		@Override
		public void onBeforeConvert(Person source) {
			invoked = true;
		}
	}
}
