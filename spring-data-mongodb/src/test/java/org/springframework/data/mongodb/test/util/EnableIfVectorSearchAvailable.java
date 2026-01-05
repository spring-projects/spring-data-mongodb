/*
 * Copyright 2024-present the original author or authors.
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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * {@link EnableIfVectorSearchAvailable} indicates a specific method can only be run in an environment that has a search
 * server available. This means that not only the mongodb instance needs to have a
 * {@literal searchIndexManagementHostAndPort} configured, but also that the search index sever is actually up and
 * running, responding to a {@literal $listSearchIndexes} aggregation.
 * <p>
 * Using this annotation will wait up to {@code 60 seconds} for the search index to become available.
 *
 * @author Christoph Strobl
 * @since 4.5.3
 * @see Tag
 */
@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Tag("vector-search")
@ExtendWith(MongoServerCondition.class)
public @interface EnableIfVectorSearchAvailable {

	/**
	 * @return the name of the collection used to run the {@literal $listSearchIndexes} aggregation.
	 */
	String collectionName() default "";

	/**
	 * @return the type for resolving the name of the collection used to run the {@literal $listSearchIndexes}
	 *         aggregation. The {@link #collectionName()} has precedence over the type.
	 */
	Class<?> collection() default Object.class;
}
