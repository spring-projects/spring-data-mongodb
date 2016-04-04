/*
 * Copyright 2011-2016 by the original author(s).
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

import org.bson.Document;
import org.junit.Test;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.mongodb.core.mapping.Account;
import org.springframework.data.mongodb.repository.Contact;
import org.springframework.data.mongodb.repository.Person;

/**
 * Unit tests for {@link AbstractMongoEventListener}.
 * 
 * @author Oliver Gierke
 * @author Martin Baumgartner
 */
public class AbstractMongoEventListenerUnitTests {

	@Test
	public void invokesCallbackForEventForPerson() {

		MongoMappingEvent<Person> event = new BeforeConvertEvent<Person>(new Person("Dave", "Matthews"), "collection-1");
		SamplePersonEventListener listener = new SamplePersonEventListener();
		listener.onApplicationEvent(event);
		assertThat(listener.invokedOnBeforeConvert, is(true));
	}

	@Test
	public void dropsEventIfNotForCorrectDomainType() {

		AbstractApplicationContext context = new ClassPathXmlApplicationContext();
		context.refresh();

		SamplePersonEventListener listener = new SamplePersonEventListener();
		context.addApplicationListener(listener);

		context.publishEvent(new BeforeConvertEvent<Person>(new Person("Dave", "Matthews"), "collection-1"));
		assertThat(listener.invokedOnBeforeConvert, is(true));

		listener.invokedOnBeforeConvert = false;
		context.publishEvent(new BeforeConvertEvent<String>("Test", "collection-1"));
		assertThat(listener.invokedOnBeforeConvert, is(false));

		context.close();
	}

	/**
	 * @see DATAMONGO-289
	 */
	@Test
	public void afterLoadEffectGetsHandledCorrectly() {

		SamplePersonEventListener listener = new SamplePersonEventListener();
		listener.onApplicationEvent(new AfterLoadEvent<Person>(new Document(), Person.class, "collection-1"));
		assertThat(listener.invokedOnAfterLoad, is(true));
	}

	/**
	 * @see DATAMONGO-289
	 */
	@Test
	public void afterLoadEventGetsFilteredForDomainType() {

		SamplePersonEventListener personListener = new SamplePersonEventListener();
		SampleAccountEventListener accountListener = new SampleAccountEventListener();
		personListener.onApplicationEvent(new AfterLoadEvent<Person>(new Document(), Person.class, "collection-1"));
		accountListener.onApplicationEvent(new AfterLoadEvent<Person>(new Document(), Person.class, "collection-1"));

		assertThat(personListener.invokedOnAfterLoad, is(true));
		assertThat(accountListener.invokedOnAfterLoad, is(false));
	}

	/**
	 * @see DATAMONGO-289
	 */
	@Test
	public void afterLoadEventGetsFilteredForDomainTypeWorksForSubtypes() {

		SamplePersonEventListener personListener = new SamplePersonEventListener();
		SampleContactEventListener contactListener = new SampleContactEventListener();
		personListener.onApplicationEvent(new AfterLoadEvent<Person>(new Document(), Person.class, "collection-1"));
		contactListener.onApplicationEvent(new AfterLoadEvent<Person>(new Document(), Person.class, "collection-1"));

		assertThat(personListener.invokedOnAfterLoad, is(true));
		assertThat(contactListener.invokedOnAfterLoad, is(true));
	}

	/**
	 * @see DATAMONGO-289
	 */
	@Test
	public void afterLoadEventGetsFilteredForDomainTypeWorksForSubtypes2() {

		SamplePersonEventListener personListener = new SamplePersonEventListener();
		SampleContactEventListener contactListener = new SampleContactEventListener();
		personListener.onApplicationEvent(new AfterLoadEvent<Contact>(new Document(), Contact.class, "collection-1"));
		contactListener.onApplicationEvent(new AfterLoadEvent<Contact>(new Document(), Contact.class, "collection-1"));

		assertThat(personListener.invokedOnAfterLoad, is(false));
		assertThat(contactListener.invokedOnAfterLoad, is(true));
	}

	/**
	 * @see DATAMONGO-333
	 */
	@Test
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void handlesUntypedImplementations() {

		UntypedEventListener listener = new UntypedEventListener();
		listener.onApplicationEvent(new MongoMappingEvent(new Object(), new Document()));
	}

	/**
	 * @see DATAMONGO-545
	 */
	@Test
	public void invokeContactCallbackForPersonEvent() {

		MongoMappingEvent<Document> event = new BeforeDeleteEvent<Person>(new Document(), Person.class, "collection-1");
		SampleContactEventListener listener = new SampleContactEventListener();
		listener.onApplicationEvent(event);

		assertThat(listener.invokedOnBeforeDelete, is(true));
	}

	/**
	 * @see DATAMONGO-545
	 */
	@Test
	public void invokePersonCallbackForPersonEvent() {

		MongoMappingEvent<Document> event = new BeforeDeleteEvent<Person>(new Document(), Person.class, "collection-1");
		SamplePersonEventListener listener = new SamplePersonEventListener();
		listener.onApplicationEvent(event);

		assertThat(listener.invokedOnBeforeDelete, is(true));
	}

	/**
	 * @see DATAMONGO-545
	 */
	@Test
	public void dontInvokePersonCallbackForAccountEvent() {

		MongoMappingEvent<Document> event = new BeforeDeleteEvent<Account>(new Document(), Account.class, "collection-1");
		SamplePersonEventListener listener = new SamplePersonEventListener();
		listener.onApplicationEvent(event);

		assertThat(listener.invokedOnBeforeDelete, is(false));
	}

	/**
	 * @see DATAMONGO-545
	 */
	@Test
	public void donInvokePersonCallbackForUntypedEvent() {

		MongoMappingEvent<Document> event = new BeforeDeleteEvent<Account>(new Document(), null, "collection-1");
		SamplePersonEventListener listener = new SamplePersonEventListener();
		listener.onApplicationEvent(event);

		assertThat(listener.invokedOnBeforeDelete, is(false));
	}

	class SamplePersonEventListener extends AbstractMongoEventListener<Person> {

		boolean invokedOnBeforeConvert;
		boolean invokedOnAfterLoad;
		boolean invokedOnBeforeDelete;
		boolean invokedOnAfterDelete;

		@Override
		public void onBeforeConvert(BeforeConvertEvent<Person> event) {
			invokedOnBeforeConvert = true;
		}

		@Override
		public void onAfterLoad(AfterLoadEvent<Person> event) {
			invokedOnAfterLoad = true;
		}

		@Override
		public void onAfterDelete(AfterDeleteEvent<Person> event) {
			invokedOnAfterDelete = true;
		}

		@Override
		public void onBeforeDelete(BeforeDeleteEvent<Person> event) {
			invokedOnBeforeDelete = true;
		}
	}

	class SampleContactEventListener extends AbstractMongoEventListener<Contact> {

		boolean invokedOnBeforeConvert;
		boolean invokedOnAfterLoad;
		boolean invokedOnBeforeDelete;
		boolean invokedOnAfterDelete;

		@Override
		public void onBeforeConvert(BeforeConvertEvent<Contact> event) {
			invokedOnBeforeConvert = true;
		}

		@Override
		public void onAfterLoad(AfterLoadEvent<Contact> event) {
			invokedOnAfterLoad = true;
		}

		@Override
		public void onAfterDelete(AfterDeleteEvent<Contact> event) {
			invokedOnAfterDelete = true;
		}

		@Override
		public void onBeforeDelete(BeforeDeleteEvent<Contact> event) {
			invokedOnBeforeDelete = true;
		}
	}

	class SampleAccountEventListener extends AbstractMongoEventListener<Account> {

		boolean invokedOnBeforeConvert;
		boolean invokedOnAfterLoad;

		@Override
		public void onBeforeConvert(BeforeConvertEvent<Account> event) {
			invokedOnBeforeConvert = true;
		}

		@Override
		public void onAfterLoad(AfterLoadEvent<Account> event) {
			invokedOnAfterLoad = true;
		}
	}

	@SuppressWarnings("rawtypes")
	class UntypedEventListener extends AbstractMongoEventListener {

	}
}
