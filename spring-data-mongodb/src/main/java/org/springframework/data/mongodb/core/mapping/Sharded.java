/*
 * Copyright 2020-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;
import org.springframework.data.annotation.Persistent;

/**
 * The {@link Sharded} annotation provides meta information about the actual distribution of data. The
 * {@link #shardKey()} is used to distribute documents across shards. <br />
 * Please see the <a href="https://docs.mongodb.com/manual/sharding/">MongoDB Documentation</a> for more information
 * about requirements and limitations of sharding.
 * <br />
 * Spring Data adds the shard key to filter queries used for
 * {@link com.mongodb.client.MongoCollection#replaceOne(org.bson.conversions.Bson, Object)} operations triggered by
 * {@code save} operations on {@link org.springframework.data.mongodb.core.MongoOperations} and
 * {@link org.springframework.data.mongodb.core.ReactiveMongoOperations} as well as {@code update/upsert} operations
 * replacing/upserting a single existing document as long as the given
 * {@link org.springframework.data.mongodb.core.query.UpdateDefinition} holds a full copy of the entity.
 * <br />
 * All other operations that require the presence of the {@literal shard key} in the filter query need to provide the
 * information via the {@link org.springframework.data.mongodb.core.query.Query} parameter when invoking the method.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 3.0
 */
@Persistent
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.ANNOTATION_TYPE })
public @interface Sharded {

	/**
	 * Alias for {@link #shardKey()}.
	 *
	 * @return {@literal _id} by default.
	 * @see #shardKey()
	 */
	@AliasFor("shardKey")
	String[] value() default {};

	/**
	 * The shard key determines the distribution of the collection's documents among the cluster's shards. The shard key
	 * is either a single or multiple indexed properties that exist in every document in the collection.
	 * <br />
	 * By default the {@literal id} property is used for sharding. <br />
	 * <strong>NOTE:</strong> Required indexes are not created automatically. Create these either externally, via
	 * {@link org.springframework.data.mongodb.core.index.IndexOperations#ensureIndex(org.springframework.data.mongodb.core.index.IndexDefinition)}
	 * or by annotating your domain model with {@link org.springframework.data.mongodb.core.index.Indexed}/
	 * {@link org.springframework.data.mongodb.core.index.CompoundIndex} along with enabled
	 * {@link org.springframework.data.mongodb.config.MongoConfigurationSupport#autoIndexCreation() auto index creation}.
	 *
	 * @return an empty key by default. Which indicates to use the entities {@literal id} property.
	 */
	@AliasFor("value")
	String[] shardKey() default {};

	/**
	 * The sharding strategy to use for distributing data across sharded clusters.
	 *
	 * @return {@link ShardingStrategy#RANGE} by default
	 */
	ShardingStrategy shardingStrategy() default ShardingStrategy.RANGE;

	/**
	 * As of MongoDB 4.2 it is possible to change the shard key using update. Using immutable shard keys avoids server
	 * round trips to obtain an entities actual shard key from the database.
	 *
	 * @return {@literal false} by default.
	 * @see <a href="https://docs.mongodb.com/manual/core/sharding-shard-key/#change-a-document-s-shard-key-value">MongoDB
	 *      Reference: Change a Document's Shard Key Value</a>
	 */
	boolean immutableKey() default false;

}
