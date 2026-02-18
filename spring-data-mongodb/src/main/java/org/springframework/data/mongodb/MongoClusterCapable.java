/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.mongodb;

/**
 * Interface that can provide access to a MongoDB cluster.
 *
 * @param <T> the MongoDB cluster/client type (e.g. {@link com.mongodb.client.MongoCluster} or
 *          {@link com.mongodb.reactivestreams.client.MongoCluster}).
 * @author Christoph Strobl
 * @since 5.1
 */
public interface MongoClusterCapable<T> {

	/**
	 * Returns the MongoDB cluster used by this factory.
	 *
	 * @return the cluster; never {@literal null}.
	 * @throws IllegalStateException if cluster cannot be obtained.
	 */
	T getMongoCluster();
}
