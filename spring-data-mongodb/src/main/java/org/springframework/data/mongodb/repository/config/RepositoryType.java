/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.mongodb.repository.config;

import java.lang.reflect.Method;

import org.springframework.data.repository.util.ReactiveWrappers;

import lombok.experimental.UtilityClass;

/**
 * Utility class to discover whether a repository interface uses reactive wrapper types.
 * 
 * @author Mark Paluch
 */
@UtilityClass
class RepositoryType {

	/**
	 * @param repositoryInterface
	 * @return {@literal true} if the {@code repositoryInterface} uses reactive wrapper types.
	 * @see ReactiveWrappers
	 * @see ReactiveWrappers#isAvailable()
	 */
	public static boolean isReactiveRepository(Class<?> repositoryInterface) {

		if (!ReactiveWrappers.isAvailable()) {
			return false;
		}

		Method[] methods = repositoryInterface.getMethods();

		for (Method method : methods) {

			if (usesReactiveWrappers(method)) {
				return true;
			}
		}

		return false;
	}

	private static boolean usesReactiveWrappers(Method method) {

		if (ReactiveWrappers.supports(method.getReturnType())) {
			return true;
		}

		for (Class<?> parameterType : method.getParameterTypes()) {
			if (ReactiveWrappers.supports(parameterType)) {
				return true;
			}
		}

		return false;
	}
}
