/*
 * Copyright 2022-2023 the original author or authors.
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
package org.springframework.data.mongodb.test.util;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Extension to consider tests that {@code @DirtiesState} and {@code @ProvidesState} through annotations.
 *
 * @author Mark Paluch
 */
public class DirtiesStateExtension implements BeforeEachCallback, AfterEachCallback {

	/**
	 * Test method that changes the data state by saving or deleting objects.
	 */
	@Retention(RetentionPolicy.RUNTIME)
	public @interface DirtiesState {

	}

	/**
	 * Test method that sets up its state within the test method itself.
	 */
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ProvidesState {

	}

	/**
	 * Interface to be implemented by tests that make use of {@link DirtiesStateExtension}.
	 */
	public interface StateFunctions {

		/**
		 * Clear the state.
		 */
		void clear();

		/**
		 * Setup the test fixture.
		 */
		void setupState();
	}

	static final String STATE_KEY = "state";

	@Override
	public void beforeEach(ExtensionContext context) throws Exception {

		Method method = context.getTestMethod().orElse(null);
		Object instance = context.getTestInstance().orElse(null);

		if (method == null || instance == null) {
			return;
		}

		if (method.isAnnotationPresent(ProvidesState.class)) {
			((StateFunctions) instance).clear();
			return;
		}

		ExtensionContext.Store mongo = getStore(context);
		Boolean state = mongo.get(STATE_KEY, Boolean.class);

		if (state == null) {

			((StateFunctions) instance).clear();
			((StateFunctions) instance).setupState();
			mongo.put(STATE_KEY, true);
		}
	}

	private ExtensionContext.Store getStore(ExtensionContext context) {
		return context.getParent().get()
				.getStore(ExtensionContext.Namespace.create("mongo-" + context.getRequiredTestClass().getName()));
	}

	@Override
	public void afterEach(ExtensionContext context) throws Exception {

		Method method = context.getTestMethod().orElse(null);

		if (method == null) {
			return;
		}

		if (method.isAnnotationPresent(DirtiesState.class) || method.isAnnotationPresent(ProvidesState.class)) {
			ExtensionContext.Store mongo = getStore(context);
			mongo.remove(STATE_KEY);
		}
	}
}
