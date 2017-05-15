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
package org.springframework.data.mongodb.core

import com.mongodb.client.result.DeleteResult
import com.mongodb.client.result.UpdateResult
import com.mongodb.reactivestreams.client.MongoCollection
import org.bson.Document
import org.springframework.data.geo.GeoResult
import org.springframework.data.mongodb.core.query.NearQuery
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import kotlin.reflect.KClass

/**
 * Extension for [ReactiveMongoOperations.indexOps] providing a [KClass] based variant.
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
fun <T : Any> ReactiveMongoOperations.indexOps(entityClass: KClass<T>): ReactiveIndexOperations =
		indexOps(entityClass.java)

/**
 * Extension for [ReactiveMongoOperations.indexOps] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
inline fun <reified T : Any> ReactiveMongoOperations.indexOps(): ReactiveIndexOperations =
		indexOps(T::class.java)

/**
 * Extension for [ReactiveMongoOperations.execute] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
inline fun <reified T : Any> ReactiveMongoOperations.execute(action: ReactiveCollectionCallback<T>): Flux<T> =
		execute(T::class.java, action)

/**
 * Extension for [ReactiveMongoOperations.createCollection] providing a [KClass] based variant.
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
fun <T : Any> ReactiveMongoOperations.createCollection(entityClass: KClass<T>,
		collectionOptions: CollectionOptions? = null): Mono<MongoCollection<Document>> =
		if (collectionOptions != null) createCollection(entityClass.java, collectionOptions) else createCollection(entityClass.java)

/**
 * Extension for [ReactiveMongoOperations.createCollection] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
inline fun <reified T : Any> ReactiveMongoOperations.createCollection(
		collectionOptions: CollectionOptions? = null): Mono<MongoCollection<Document>> =
		if (collectionOptions != null) createCollection(T::class.java, collectionOptions) else createCollection(T::class.java)

/**
 * Extension for [ReactiveMongoOperations.collectionExists] providing a [KClass] based variant.
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
fun <T : Any> ReactiveMongoOperations.collectionExists(entityClass: KClass<T>): Mono<Boolean> =
		collectionExists(entityClass.java)

/**
 * Extension for [ReactiveMongoOperations.collectionExists] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
inline fun <reified T : Any> ReactiveMongoOperations.collectionExists(): Mono<Boolean> =
		collectionExists(T::class.java)

/**
 * Extension for [ReactiveMongoOperations.dropCollection] providing a [KClass] based variant.
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
fun <T : Any> ReactiveMongoOperations.dropCollection(entityClass: KClass<T>): Mono<Void> =
		dropCollection(entityClass.java)

/**
 * Extension for [ReactiveMongoOperations.dropCollection] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
inline fun <reified T : Any> ReactiveMongoOperations.dropCollection(): Mono<Void> =
		dropCollection(T::class.java)


/**
 * Extension for [ReactiveMongoOperations.findAll] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
inline fun <reified T : Any> ReactiveMongoOperations.findAll(collectionName: String? = null): Flux<T> =
		if (collectionName != null) findAll(T::class.java, collectionName) else findAll(T::class.java)

/**
 * Extension for [ReactiveMongoOperations.findOne] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
inline fun <reified T : Any> ReactiveMongoOperations.findOne(query: Query, collectionName: String? = null): Mono<T> =
		if (collectionName != null) findOne(query, T::class.java, collectionName) else findOne(query, T::class.java)

/**
 * Extension for [ReactiveMongoOperations.exists] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
fun <T : Any> ReactiveMongoOperations.exists(query: Query, entityClass: KClass<T>,
		collectionName: String? = null): Mono<Boolean> =
		if (collectionName != null) exists(query, entityClass.java, collectionName) else exists(query, entityClass.java)

/**
 * Extension for [ReactiveMongoOperations.exists] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> ReactiveMongoOperations.exists(query: Query,
		collectionName: String? = null): Mono<Boolean> =
		if (collectionName != null) exists(query, T::class.java, collectionName) else exists(query, T::class.java)

/**
 * Extension for [ReactiveMongoOperations.find] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
inline fun <reified T : Any> ReactiveMongoOperations.find(query: Query, collectionName: String? = null): Flux<T> =
		if (collectionName != null) find(query, T::class.java, collectionName) else find(query, T::class.java)

/**
 * Extension for [ReactiveMongoOperations.findById] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
inline fun <reified T : Any> ReactiveMongoOperations.findById(id: Any, collectionName: String? = null): Mono<T> =
		if (collectionName != null) findById(id, T::class.java, collectionName) else findById(id, T::class.java)

/**
 * Extension for [ReactiveMongoOperations.geoNear] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
inline fun <reified T : Any> ReactiveMongoOperations.geoNear(near: NearQuery,
		collectionName: String? = null): Flux<GeoResult<T>> =
		if (collectionName != null) geoNear(near, T::class.java, collectionName) else geoNear(near, T::class.java)

/**
 * Extension for [ReactiveMongoOperations.findAndModify] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
inline fun <reified T : Any> ReactiveMongoOperations.findAndModify(query: Query,
		update: Update, options: FindAndModifyOptions, collectionName: String? = null): Mono<T> =
		if (collectionName != null) findAndModify(query, update, options, T::class.java, collectionName) else findAndModify(query, update, options, T::class.java)

/**
 * Extension for [ReactiveMongoOperations.findAndRemove] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
inline fun <reified T : Any> ReactiveMongoOperations.findAndRemove(query: Query,
		collectionName: String? = null): Mono<T> =
		if (collectionName != null) findAndRemove(query, T::class.java, collectionName)
		else findAndRemove(query, T::class.java)

/**
 * Extension for [ReactiveMongoOperations.count] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
fun <T : Any> ReactiveMongoOperations.count(query: Query = Query(), entityClass: KClass<T>,
		collectionName: String? = null): Mono<Long> =
		if (collectionName != null) count(query, entityClass.java, collectionName)
		else count(query, entityClass.java)

/**
 * Extension for [ReactiveMongoOperations.count] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> ReactiveMongoOperations.count(query: Query = Query(),
		collectionName: String? = null): Mono<Long> =
		if (collectionName != null) count(query, T::class.java, collectionName)
		else count(query, T::class.java)

/**
 * Extension for [ReactiveMongoOperations.insert] providing a [KClass] based variant.
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
fun <T : Any> ReactiveMongoOperations.insert(batchToSave: Collection<T>, entityClass: KClass<T>): Flux<T> =
	   insert(batchToSave, entityClass.java)

/**
 * Extension for [ReactiveMongoOperations.insertAll] providing a [KClass] based variant.
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
fun <T : Any> ReactiveMongoOperations.insertAll(batchToSave: Mono<out Collection<T>>,
		entityClass: KClass<T>): Flux<T> =
		insertAll(batchToSave, entityClass.java)

/**
 * Extension for [ReactiveMongoOperations.upsert] providing a [KClass] based variant.
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
fun <T : Any> ReactiveMongoOperations.upsert(query: Query, update: Update, entityClass: KClass<T>,
		collectionName: String? = null): Mono<UpdateResult> =
		if (collectionName != null) upsert(query, update, entityClass.java, collectionName) else upsert(query, update, entityClass.java)

/**
 * Extension for [ReactiveMongoOperations.upsert] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> ReactiveMongoOperations.upsert(query: Query, update: Update,
		collectionName: String? = null): Mono<UpdateResult> =
		if (collectionName != null) upsert(query, update, T::class.java, collectionName)
		else upsert(query, update, T::class.java)

/**
 * Extension for [ReactiveMongoOperations.updateFirst] providing a [KClass] based variant.
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
fun <T : Any> ReactiveMongoOperations.updateFirst(query: Query, update: Update,
		entityClass: KClass<T>, collectionName: String? = null): Mono<UpdateResult> =
		if (collectionName != null) updateFirst(query, update, entityClass.java, collectionName)
		else updateFirst(query, update, entityClass.java)

/**
 * Extension for [ReactiveMongoOperations.updateFirst] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> ReactiveMongoOperations.updateFirst(query: Query, update: Update,
		collectionName: String? = null): Mono<UpdateResult> =
		if (collectionName != null) updateFirst(query, update, T::class.java, collectionName)
		else updateFirst(query, update, T::class.java)

/**
 * Extension for [ReactiveMongoOperations.updateMulti] providing a [KClass] based variant.
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
fun <T : Any> ReactiveMongoOperations.updateMulti(query: Query, update: Update,
		entityClass: KClass<T>, collectionName: String? = null): Mono<UpdateResult> =
		if (collectionName != null) updateMulti(query, update, entityClass.java, collectionName)
		else updateMulti(query, update, entityClass.java)

/**
 * Extension for [ReactiveMongoOperations.updateMulti] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> ReactiveMongoOperations.updateMulti(query: Query, update: Update,
		collectionName: String? = null): Mono<UpdateResult> =
		if (collectionName != null) updateMulti(query, update, T::class.java, collectionName)
		else updateMulti(query, update, T::class.java)

/**
 * Extension for [ReactiveMongoOperations.remove] providing a [KClass] based variant.
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
fun <T : Any> ReactiveMongoOperations.remove(query: Query, entityClass: KClass<T>,
		collectionName: String? = null): Mono<DeleteResult> =
		if (collectionName != null) remove(query, entityClass.java, collectionName)
		else remove(query, entityClass.java)

/**
 * Extension for [ReactiveMongoOperations.remove] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> ReactiveMongoOperations.remove(query: Query,
		collectionName: String? = null): Mono<DeleteResult> =
		if (collectionName != null) remove(query, T::class.java, collectionName)
		else remove(query, T::class.java)

/**
 * Extension for [ReactiveMongoOperations.findAllAndRemove] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> ReactiveMongoOperations.findAllAndRemove(query: Query): Flux<T> =
		findAllAndRemove(query, T::class.java)

/**
 * Extension for [ReactiveMongoOperations.tail] avoiding requiring the type parameter
 * thanks to Kotlin reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 5.0
 */
inline fun <reified T : Any> ReactiveMongoOperations.tail(query: Query, collectionName: String? = null): Flux<T> =
		if (collectionName != null) tail(query, T::class.java, collectionName) else tail(query, T::class.java)
