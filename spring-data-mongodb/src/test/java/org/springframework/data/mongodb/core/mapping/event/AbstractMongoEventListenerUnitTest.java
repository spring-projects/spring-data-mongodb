/*
 * Copyright 2011 by the original author(s).
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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

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
		assertThat(listener.invokedOnBeforeConvert, is(true));
	}
	
	@Test
	public void dropsEventIfNotForCorrectDomainType() {
		
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext();
		context.refresh();
		
		SampleEventListener listener = new SampleEventListener();
		context.addApplicationListener(listener);
		
		context.publishEvent(new BeforeConvertEvent<Person>(new Person("Dave", "Matthews")));
		assertThat(listener.invokedOnBeforeConvert, is(true));
		
		listener.invokedOnBeforeConvert = false;
		context.publishEvent(new BeforeConvertEvent<String>("Test"));
		assertThat(listener.invokedOnBeforeConvert, is(false));
	}
	
	/**
	 * @see DATADOC-289
	 */
	@Test
	public void afterLoadEffectGetsHandledCorrectly() {
		
		SampleEventListener listener = new SampleEventListener();
		listener.onApplicationEvent(new AfterLoadEvent(new BasicDBObject()));
		assertThat(listener.invokedOnAfterLoad, is(true));
	}
	

	class SampleEventListener extends AbstractMongoEventListener<Person> {
		
		boolean invokedOnBeforeConvert;
		boolean invokedOnAfterLoad;
		
		@Override
		public void onBeforeConvert(Person source) {
			invokedOnBeforeConvert = true;
		}
		
		@Override
		public void onAfterLoad(DBObject dbo) {
			invokedOnAfterLoad = true;
		}
	}
}
