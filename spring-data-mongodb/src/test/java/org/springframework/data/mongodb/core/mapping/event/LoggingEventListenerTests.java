/*
 * Copyright 2017 the original author or authors.
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

import static org.hamcrest.core.StringStartsWith.*;
import static org.junit.Assert.*;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * @author Christoph Strobl
 */
public class LoggingEventListenerTests {

	LoggingEventListener listener;
	ListAppender<ILoggingEvent> appender;

	@Before
	public void setUp() {

		// set log level for LoggingEventListener to "info" and set up an appender capturing events.
		ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory
				.getLogger(LoggingEventListener.class);
		logger.setLevel(Level.toLevel("info"));

		appender = new ListAppender();
		logger.addAppender(appender);
		appender.start();

		listener = new LoggingEventListener();
	}

	@Test // DATAMONGO-1645
	public void shouldSerializeAfterConvertEventCorrectly() {

		listener.onAfterConvert(new AfterConvertEvent<Object>(new Document("foo", new Foo()), this, "collection"));

		assertThat(appender.list.get(0).getFormattedMessage(), startsWith("onAfterConvert: { \"foo\""));
	}

	@Test // DATAMONGO-1645
	public void shouldSerializeBeforeSaveEventEventCorrectly() {

		listener.onBeforeSave(new BeforeSaveEvent<Object>(new Foo(), new Document("foo", new Foo()), "collection"));

		assertThat(appender.list.get(0).getFormattedMessage(),
				startsWith("onBeforeSave: org.springframework.data.mongodb.core."));
	}

	@Test // DATAMONGO-1645
	public void shouldSerializeAfterSaveEventEventCorrectly() {

		listener.onAfterSave(new AfterSaveEvent<Object>(new Foo(), new Document("foo", new Foo()), "collection"));

		assertThat(appender.list.get(0).getFormattedMessage(),
				startsWith("onAfterSave: org.springframework.data.mongodb.core."));
	}

	@Test // DATAMONGO-1645
	public void shouldSerializeBeforeDeleteEventEventCorrectly() {

		listener.onBeforeDelete(new BeforeDeleteEvent<Object>(new Document("foo", new Foo()), Object.class, "collection"));

		assertThat(appender.list.get(0).getFormattedMessage(), startsWith("onBeforeDelete: { \"foo\""));
	}

	@Test // DATAMONGO-1645
	public void shouldSerializeAfterDeleteEventEventCorrectly() {

		listener.onAfterDelete(new AfterDeleteEvent<Object>(new Document("foo", new Foo()), Object.class, "collection"));

		assertThat(appender.list.get(0).getFormattedMessage(), startsWith("onAfterDelete: { \"foo\""));
	}

	static class Foo {

	}

}
