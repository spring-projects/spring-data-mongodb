/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping.event;

import static org.assertj.core.api.Assertions.*;

import java.time.Instant;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.mongodb.core.mapping.Account;
import org.springframework.data.mongodb.repository.Contact;
import org.springframework.data.mongodb.repository.Person;

import com.mongodb.BasicDBObject;

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
		assertThat(listener.invokedOnBeforeConvert).isTrue();
	}

	@Test
	public void dropsEventIfNotForCorrectDomainType() {

		AbstractApplicationContext context = new ClassPathXmlApplicationContext();
		context.refresh();

		SamplePersonEventListener listener = new SamplePersonEventListener();
		context.addApplicationListener(listener);

		context.publishEvent(new BeforeConvertEvent<Person>(new Person("Dave", "Matthews"), "collection-1"));
		assertThat(listener.invokedOnBeforeConvert).isTrue();

		listener.invokedOnBeforeConvert = false;
		context.publishEvent(new BeforeConvertEvent<String>("Test", "collection-1"));
		assertThat(listener.invokedOnBeforeConvert).isFalse();

		context.close();
	}

	@Test // DATAMONGO-289
	public void afterLoadEffectGetsHandledCorrectly() {

		SamplePersonEventListener listener = new SamplePersonEventListener();
		listener.onApplicationEvent(new AfterLoadEvent<Person>(new Document(), Person.class, "collection-1"));
		assertThat(listener.invokedOnAfterLoad).isTrue();
	}

	@Test // DATAMONGO-289
	public void afterLoadEventGetsFilteredForDomainType() {

		SamplePersonEventListener personListener = new SamplePersonEventListener();
		SampleAccountEventListener accountListener = new SampleAccountEventListener();
		personListener.onApplicationEvent(new AfterLoadEvent<Person>(new Document(), Person.class, "collection-1"));
		accountListener.onApplicationEvent(new AfterLoadEvent<Person>(new Document(), Person.class, "collection-1"));

		assertThat(personListener.invokedOnAfterLoad).isTrue();
		assertThat(accountListener.invokedOnAfterLoad).isFalse();
	}

	@Test // DATAMONGO-289
	public void afterLoadEventGetsFilteredForDomainTypeWorksForSubtypes() {

		SamplePersonEventListener personListener = new SamplePersonEventListener();
		SampleContactEventListener contactListener = new SampleContactEventListener();
		personListener.onApplicationEvent(new AfterLoadEvent<Person>(new Document(), Person.class, "collection-1"));
		contactListener.onApplicationEvent(new AfterLoadEvent<Person>(new Document(), Person.class, "collection-1"));

		assertThat(personListener.invokedOnAfterLoad).isTrue();
		assertThat(contactListener.invokedOnAfterLoad).isTrue();
	}

	@Test // DATAMONGO-289
	public void afterLoadEventGetsFilteredForDomainTypeWorksForSubtypes2() {

		SamplePersonEventListener personListener = new SamplePersonEventListener();
		SampleContactEventListener contactListener = new SampleContactEventListener();
		personListener.onApplicationEvent(new AfterLoadEvent<Contact>(new Document(), Contact.class, "collection-1"));
		contactListener.onApplicationEvent(new AfterLoadEvent<Contact>(new Document(), Contact.class, "collection-1"));

		assertThat(personListener.invokedOnAfterLoad).isFalse();
		assertThat(contactListener.invokedOnAfterLoad).isTrue();
	}

	@Test // DATAMONGO-333
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void handlesUntypedImplementations() {

		UntypedEventListener listener = new UntypedEventListener();
		listener.onApplicationEvent(new MongoMappingEvent(new Object(), new Document(), "collection-1"));
	}

	@Test // DATAMONGO-545
	public void invokeContactCallbackForPersonEvent() {

		MongoMappingEvent<Document> event = new BeforeDeleteEvent<Person>(new Document(), Person.class, "collection-1");
		SampleContactEventListener listener = new SampleContactEventListener();
		listener.onApplicationEvent(event);

		assertThat(listener.invokedOnBeforeDelete).isTrue();
	}

	@Test // DATAMONGO-545
	public void invokePersonCallbackForPersonEvent() {

		MongoMappingEvent<Document> event = new BeforeDeleteEvent<Person>(new Document(), Person.class, "collection-1");
		SamplePersonEventListener listener = new SamplePersonEventListener();
		listener.onApplicationEvent(event);

		assertThat(listener.invokedOnBeforeDelete).isTrue();
	}

	@Test // DATAMONGO-545
	public void dontInvokePersonCallbackForAccountEvent() {

		MongoMappingEvent<Document> event = new BeforeDeleteEvent<Account>(new Document(), Account.class, "collection-1");
		SamplePersonEventListener listener = new SamplePersonEventListener();
		listener.onApplicationEvent(event);

		assertThat(listener.invokedOnBeforeDelete).isFalse();
	}

	@Test // DATAMONGO-545
	public void donInvokePersonCallbackForUntypedEvent() {

		MongoMappingEvent<Document> event = new BeforeDeleteEvent<Account>(new Document(), null, "collection-1");
		SamplePersonEventListener listener = new SamplePersonEventListener();
		listener.onApplicationEvent(event);

		assertThat(listener.invokedOnBeforeDelete).isFalse();
	}

	@Test // GH-3968
	public void debugLogShouldNotFailMongoDBCodecError() {

		MongoMappingEvent<BasicDBObject> event = new BeforeConvertEvent<>(new BasicDBObject("date", Instant.now()), "collection-1");
		UntypedEventListener listener = new UntypedEventListener();
		listener.onApplicationEvent(event);
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
