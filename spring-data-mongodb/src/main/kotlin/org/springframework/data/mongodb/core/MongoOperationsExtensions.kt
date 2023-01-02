/*
 * Copyright 2017-2023 the original author or authors.
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
package org.springframework.data.mongodb.core

import com.mongodb.client.MongoCollection
import com.mongodb.client.result.DeleteResult
import com.mongodb.client.result.UpdateResult
import org.bson.Document
import org.springframework.data.geo.GeoResults
import org.springframework.data.mongodb.core.BulkOperations.BulkMode
import org.springframework.data.mongodb.core.aggregation.Aggregation
import org.springframework.data.mongodb.core.aggregation.AggregationResults
import org.springframework.data.mongodb.core.index.IndexOperations
import org.springframework.data.mongodb.core.mapreduce.MapReduceOptions
import org.springframework.data.mongodb.core.mapreduce.MapReduceResults
import org.springframework.data.mongodb.core.query.NearQuery
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.UpdateDefinition
import java.util.stream.Stream

/**
 * Extension for [MongoOperations.getCollectionName] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
inline fun <reified T : Any> MongoOperations.getCollectionName(): String =
		getCollectionName(T::class.java)

/**
 * Extension for [MongoOperations.execute] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
inline fun <reified T : Any> MongoOperations.execute(action: CollectionCallback<T>): T? =
		execute(T::class.java, action)

/**
 * Extension for [MongoOperations.stream] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
inline fun <reified T : Any> MongoOperations.stream(query: Query): Stream<T> =
	stream(query, T::class.java)

/**
 * Extension for [MongoOperations.stream] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
inline fun <reified T : Any> MongoOperations.stream(
	query: Query,
	collectionName: String? = null
): Stream<T> =
	if (collectionName != null) stream(query, T::class.java, collectionName)
	else stream(query, T::class.java)

/**
 * Extension for [MongoOperations.createCollection] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
inline fun <reified T : Any> MongoOperations.createCollection(
		collectionOptions: CollectionOptions? = null): MongoCollection<Document> =
		if (collectionOptions != null) createCollection(T::class.java, collectionOptions)
		else createCollection(T::class.java)

/**
 * Extension for [MongoOperations.collectionExists] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
inline fun <reified T : Any> MongoOperations.collectionExists(): Boolean =
		collectionExists(T::class.java)

/**
 * Extension for [MongoOperations.dropCollection] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
inline fun <reified T : Any> MongoOperations.dropCollection() {
	dropCollection(T::class.java)
}

/**
 * Extension for [MongoOperations.indexOps] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
inline fun <reified T : Any> MongoOperations.indexOps(): IndexOperations =
		indexOps(T::class.java)

/**
 * Extension for [MongoOperations.bulkOps] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> MongoOperations.bulkOps(bulkMode: BulkMode, collectionName: String? = null): BulkOperations =
		if (collectionName != null) bulkOps(bulkMode, T::class.java, collectionName)
		else bulkOps(bulkMode, T::class.java)

/**
 * Extension for [MongoOperations.findAll] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
inline fun <reified T : Any> MongoOperations.findAll(collectionName: String? = null): List<T> =
		if (collectionName != null) findAll(T::class.java, collectionName) else findAll(T::class.java)

/**
 * Extension for [MongoOperations.aggregate] leveraging reified type parameters.
 *
 * @author Mark Paluch
 * @since 3.2
 */
inline fun <reified I : Any, reified O : Any> MongoOperations.aggregate(aggregation: Aggregation): AggregationResults<O> =
	aggregate(aggregation, I::class.java, O::class.java)

/**
 * Extension for [MongoOperations.aggregate] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
inline fun <reified O : Any> MongoOperations.aggregate(
	aggregation: Aggregation,
	collectionName: String
): AggregationResults<O> =
	aggregate(aggregation, collectionName, O::class.java)

/**
 * Extension for [MongoOperations.aggregateStream] leveraging reified type parameters.
 *
 * @author Mark Paluch
 * @since 3.2
 */
inline fun <reified I : Any, reified O : Any> MongoOperations.aggregateStream(aggregation: Aggregation): Stream<O> =
	aggregateStream(aggregation, I::class.java, O::class.java)

/**
 * Extension for [MongoOperations.aggregateStream] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
inline fun <reified O : Any> MongoOperations.aggregateStream(
	aggregation: Aggregation,
	collectionName: String
): Stream<O> =
	aggregateStream(aggregation, collectionName, O::class.java)

/**
 * Extension for [MongoOperations.mapReduce] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
inline fun <reified T : Any> MongoOperations.mapReduce(collectionName: String, mapFunction: String, reduceFunction: String, options: MapReduceOptions? = null): MapReduceResults<T> =
		if (options != null) mapReduce(collectionName, mapFunction, reduceFunction, options, T::class.java)
		else mapReduce(collectionName, mapFunction, reduceFunction, T::class.java)

/**
 * Extension for [MongoOperations.mapReduce] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 52.0
 */
inline fun <reified T : Any> MongoOperations.mapReduce(query: Query, collectionName: String, mapFunction: String, reduceFunction: String, options: MapReduceOptions? = null): MapReduceResults<T> =
		if (options != null) mapReduce(query, collectionName, mapFunction, reduceFunction, options, T::class.java)
		else mapReduce(query, collectionName, mapFunction, reduceFunction, T::class.java)

/**
 * Extension for [MongoOperations.geoNear] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
@Suppress("DEPRECATION")
@Deprecated("Since 2.2, the `geoNear` command has been removed in MongoDB Server 4.2.0. Use Aggregations with `Aggregation.geoNear(NearQuery, String)` instead.", replaceWith = ReplaceWith("aggregate<T>()"))
inline fun <reified T : Any> MongoOperations.geoNear(near: NearQuery, collectionName: String? = null): GeoResults<T> =
		if (collectionName != null) geoNear(near, T::class.java, collectionName)
		else geoNear(near, T::class.java)

/**
 * Extension for [MongoOperations.findOne] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
inline fun <reified T : Any> MongoOperations.findOne(query: Query, collectionName: String? = null): T? =
		if (collectionName != null) findOne(query, T::class.java, collectionName) else findOne(query, T::class.java)

/**
 * Extension for [MongoOperations.exists] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> MongoOperations.exists(query: Query, collectionName: String? = null): Boolean =
		if (collectionName != null) exists(query, T::class.java, collectionName)
		else exists(query, T::class.java)

/**
 * Extension for [MongoOperations.find] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
inline fun <reified T : Any> MongoOperations.find(query: Query, collectionName: String? = null): List<T> =
		if (collectionName != null) find(query, T::class.java, collectionName)
		else find(query, T::class.java)

/**
 * Extension for [MongoOperations.findById] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
inline fun <reified T : Any> MongoOperations.findById(id: Any, collectionName: String? = null): T? =
		if (collectionName != null) findById(id, T::class.java, collectionName)
		else findById(id, T::class.java)

/**
 * Extension for [MongoOperations.findDistinct] leveraging reified type parameters.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
inline fun <reified T : Any, reified E : Any> MongoOperations.findDistinct(query: Query, field: String, collectionName: String? = null): List<T> =
		if (collectionName != null) findDistinct(query, field, collectionName, E::class.java, T::class.java)
		else findDistinct(query, field, E::class.java, T::class.java)

/**
 * Extension for [MongoOperations.findAndModify] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
inline fun <reified T : Any> MongoOperations.findAndModify(query: Query, update: UpdateDefinition, options: FindAndModifyOptions, collectionName: String? = null): T? =
		if (collectionName != null) findAndModify(query, update, options, T::class.java, collectionName)
		else findAndModify(query, update, options, T::class.java)

/**
 * Extension for [MongoOperations.findAndRemove] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
inline fun <reified T : Any> MongoOperations.findAndRemove(query: Query, collectionName: String? = null): T? =
		if (collectionName != null) findAndRemove(query, T::class.java, collectionName)
		else findAndRemove(query, T::class.java)

/**
 * Extension for [MongoOperations.count] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> MongoOperations.count(query: Query = Query(), collectionName: String? = null): Long =
		if (collectionName != null) count(query, T::class.java, collectionName) else count(query, T::class.java)

/**
 * Extension for [MongoOperations.insert] leveraging reified type parameters.
 *
 * @author Mark Paluch
 * @since 2.2
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> MongoOperations.insert(batchToSave: Collection<T>): Collection<T> = insert(batchToSave, T::class.java)

/**
 * Extension for [MongoOperations.upsert] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> MongoOperations.upsert(query: Query, update: UpdateDefinition, collectionName: String? = null): UpdateResult =
		if (collectionName != null) upsert(query, update, T::class.java, collectionName)
		else upsert(query, update, T::class.java)

/**
 * Extension for [MongoOperations.updateFirst] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> MongoOperations.updateFirst(query: Query, update: UpdateDefinition, collectionName: String? = null): UpdateResult =
		if (collectionName != null) updateFirst(query, update, T::class.java, collectionName)
		else updateFirst(query, update, T::class.java)

/**
 * Extension for [MongoOperations.updateMulti] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> MongoOperations.updateMulti(query: Query, update: UpdateDefinition, collectionName: String? = null): UpdateResult =
		if (collectionName != null) updateMulti(query, update, T::class.java, collectionName)
		else updateMulti(query, update, T::class.java)

/**
 * Extension for [MongoOperations.remove] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> MongoOperations.remove(query: Query, collectionName: String? = null): DeleteResult =
		if (collectionName != null) remove(query, T::class.java, collectionName)
		else remove(query, T::class.java)

/**
 * Extension for [MongoOperations.findAllAndRemove] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
inline fun <reified T : Any> MongoOperations.findAllAndRemove(query: Query): List<T> =
		findAllAndRemove(query, T::class.java)
