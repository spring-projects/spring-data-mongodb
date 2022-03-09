/*
 * Copyright 2018-2022 the original author or authors.
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
package org.springframework.data.mongodb.core.query

import java.util.regex.Pattern

import org.springframework.data.geo.Circle
import org.springframework.data.geo.Point
import org.springframework.data.geo.Shape
import org.springframework.data.mongodb.core.geo.GeoJson
import org.springframework.data.mongodb.core.schema.JsonSchemaObject

import com.querydsl.core.types.Path

import org.bson.BsonRegularExpression

/**
 * Creates a criterion using equality.
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.isEqualTo
 */
infix fun <T> Path<T>.isEqualTo(value: T) =
    Criteria(this.asName()).isEqualTo(value)

/**
 * Creates a criterion using the $ne operator.
 *
 * See [MongoDB Query operator: $ne](https://docs.mongodb.com/manual/reference/operator/query/ne/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.ne
 */
infix fun <T> Path<T>.ne(value: T): Criteria =
    Criteria(this.asName()).ne(value)

/**
 * Creates a criterion using the $lt operator.
 *
 * See [MongoDB Query operator: $lt](https://docs.mongodb.com/manual/reference/operator/query/lt/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.lt
 */
infix fun <T> Path<T>.lt(value: T): Criteria =
    Criteria(this.asName()).lt(value)

/**
 * Creates a criterion using the $lte operator.
 *
 * See [MongoDB Query operator: $lte](https://docs.mongodb.com/manual/reference/operator/query/lte/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.lte
 */
infix fun <T> Path<T>.lte(value: T): Criteria =
    Criteria(this.asName()).lte(value)

/**
 * Creates a criterion using the $gt operator.
 *
 * See [MongoDB Query operator: $gt](https://docs.mongodb.com/manual/reference/operator/query/gt/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.gt
 */
infix fun <T> Path<T>.gt(value: T): Criteria =
    Criteria(this.asName()).gt(value)

/**
 * Creates a criterion using the $gte operator.
 *
 * See [MongoDB Query operator: $gte](https://docs.mongodb.com/manual/reference/operator/query/gte/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.gte
 */
infix fun <T> Path<T>.gte(value: T): Criteria =
    Criteria(this.asName()).gte(value)

/**
 * Creates a criterion using the $in operator.
 *
 * See [MongoDB Query operator: $in](https://docs.mongodb.com/manual/reference/operator/query/in/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.inValues
 */
fun <T> Path<T>.inValues(vararg o: Any): Criteria =
    Criteria(this.asName()).`in`(*o)

/**
 * Creates a criterion using the $in operator.
 *
 * See [MongoDB Query operator: $in](https://docs.mongodb.com/manual/reference/operator/query/in/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.inValues
 */
infix fun <T> Path<T>.inValues(value: Collection<T>): Criteria =
    Criteria(this.asName()).`in`(value)

/**
 * Creates a criterion using the $nin operator.
 *
 * See [MongoDB Query operator: $nin](https://docs.mongodb.com/manual/reference/operator/query/nin/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.nin
 */
fun <T> Path<T>.nin(vararg o: Any): Criteria =
    Criteria(this.asName()).nin(*o)

/**
 * Creates a criterion using the $nin operator.
 *
 * See [MongoDB Query operator: $nin](https://docs.mongodb.com/manual/reference/operator/query/nin/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.nin
 */
infix fun <T> Path<T>.nin(value: Collection<T>): Criteria =
    Criteria(this.asName()).nin(value)

/**
 * Creates a criterion using the $all operator.
 *
 * See [MongoDB Query operator: $all](https://docs.mongodb.com/manual/reference/operator/query/all/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.all
 */
fun Path<*>.all(vararg o: Any): Criteria =
    Criteria(this.asName()).all(*o)

/**
 * Creates a criterion using the $all operator.
 *
 * See [MongoDB Query operator: $all](https://docs.mongodb.com/manual/reference/operator/query/all/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.all
 */
infix fun Path<*>.all(value: Collection<*>): Criteria =
    Criteria(this.asName()).all(value)

/**
 * Creates a criterion using the $size operator.
 *
 * See [MongoDB Query operator: $size](https://docs.mongodb.com/manual/reference/operator/query/size/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.size
 */
infix fun Path<*>.size(s: Int): Criteria =
    Criteria(this.asName()).size(s)

/**
 * Creates a criterion using the $exists operator.
 *
 * See [MongoDB Query operator: $exists](https://docs.mongodb.com/manual/reference/operator/query/exists/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.exists
 */
infix fun Path<*>.exists(b: Boolean): Criteria =
    Criteria(this.asName()).exists(b)

/**
 * Creates a criterion using the $type operator.
 *
 * See [MongoDB Query operator: $type](https://docs.mongodb.com/manual/reference/operator/query/type/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.type
 */
infix fun Path<*>.type(t: Int): Criteria =
    Criteria(this.asName()).type(t)

/**
 * Creates a criterion using the $type operator.
 *
 * See [MongoDB Query operator: $type](https://docs.mongodb.com/manual/reference/operator/query/type/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.type
 */
infix fun Path<*>.type(t: Collection<JsonSchemaObject.Type>): Criteria =
    Criteria(this.asName()).type(*t.toTypedArray())

/**
 * Creates a criterion using the $type operator.
 *
 * See [MongoDB Query operator: $type](https://docs.mongodb.com/manual/reference/operator/query/type/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.type
 */
fun Path<*>.type(vararg t: JsonSchemaObject.Type): Criteria =
    Criteria(this.asName()).type(*t)

/**
 * Creates a criterion using the $not meta operator which affects the clause directly following
 *
 * See [MongoDB Query operator: $not](https://docs.mongodb.com/manual/reference/operator/query/not/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.not
 */
fun Path<*>.not(): Criteria =
    Criteria(this.asName()).not()

/**
 * Creates a criterion using a $regex operator.
 *
 * See [MongoDB Query operator: $regex](https://docs.mongodb.com/manual/reference/operator/query/regex/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.regex
 */
infix fun Path<String?>.regex(re: String): Criteria =
    Criteria(this.asName()).regex(re, null)

/**
 * Creates a criterion using a $regex and $options operator.
 *
 * See [MongoDB Query operator: $regex](https://docs.mongodb.com/manual/reference/operator/query/regex/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.regex
 */
fun Path<String?>.regex(re: String, options: String?): Criteria =
    Criteria(this.asName()).regex(re, options)

/**
 * Syntactical sugar for [isEqualTo] making obvious that we create a regex predicate.
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.regex
 */
infix fun Path<String?>.regex(re: Regex): Criteria =
    Criteria(this.asName()).regex(re.toPattern())

/**
 * Syntactical sugar for [isEqualTo] making obvious that we create a regex predicate.
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.regex
 */
infix fun Path<String?>.regex(re: Pattern): Criteria =
    Criteria(this.asName()).regex(re)

/**
 * Syntactical sugar for [isEqualTo] making obvious that we create a regex predicate.
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.regex
 */
infix fun Path<String?>.regex(re: BsonRegularExpression): Criteria =
    Criteria(this.asName()).regex(re)

/**
 * Creates a geospatial criterion using a $geoWithin $centerSphere operation. This is only available for
 * Mongo 2.4 and higher.
 *
 * See [MongoDB Query operator:
 * $geoWithin](https://docs.mongodb.com/manual/reference/operator/query/geoWithin/)
 *
 * See [MongoDB Query operator:
 * $centerSphere](https://docs.mongodb.com/manual/reference/operator/query/centerSphere/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.withinSphere
 */
infix fun Path<GeoJson<*>>.withinSphere(circle: Circle): Criteria =
    Criteria(this.asName()).withinSphere(circle)

/**
 * Creates a geospatial criterion using a $geoWithin operation.
 *
 * See [MongoDB Query operator:
 * $geoWithin](https://docs.mongodb.com/manual/reference/operator/query/geoWithin/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.within
 */
infix fun Path<GeoJson<*>>.within(shape: Shape): Criteria =
    Criteria(this.asName()).within(shape)

/**
 * Creates a geospatial criterion using a $near operation.
 *
 * See [MongoDB Query operator: $near](https://docs.mongodb.com/manual/reference/operator/query/near/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.near
 */
infix fun Path<GeoJson<*>>.near(point: Point): Criteria =
    Criteria(this.asName()).near(point)

/**
 * Creates a geospatial criterion using a $nearSphere operation. This is only available for Mongo 1.7 and
 * higher.
 *
 * See [MongoDB Query operator:
 * $nearSphere](https://docs.mongodb.com/manual/reference/operator/query/nearSphere/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.nearSphere
 */
infix fun Path<GeoJson<*>>.nearSphere(point: Point): Criteria =
    Criteria(this.asName()).nearSphere(point)

/**
 * Creates criterion using `$geoIntersects` operator which matches intersections of the given `geoJson`
 * structure and the documents one. Requires MongoDB 2.4 or better.
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.intersects
 */
infix fun Path<GeoJson<*>>.intersects(geoJson: GeoJson<*>): Criteria =
    Criteria(this.asName()).intersects(geoJson)

/**
 * Creates a criterion using the $elemMatch operator
 *
 * See [MongoDB Query operator:
 * $elemMatch](https://docs.mongodb.com/manual/reference/operator/query/elemMatch/)
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.elemMatch
 */
infix fun Path<*>.elemMatch(c: Criteria): Criteria =
    Criteria(this.asName()).elemMatch(c)

/**
 * Use [Criteria.BitwiseCriteriaOperators] as gateway to create a criterion using one of the
 * [bitwise operators](https://docs.mongodb.com/manual/reference/operator/query-bitwise/) like
 * `$bitsAllClear`.
 *
 * Example:
 * ```
 * bits { allClear(123) }
 * ```
 * @author Sangyong choi
 * @since 4.0
 * @see Criteria.bits
 */
infix fun Path<*>.bits(bitwiseCriteria: Criteria.BitwiseCriteriaOperators.() -> Criteria) =
    Criteria(this.asName()).bits().let(bitwiseCriteria)

private fun Path<*>.asName(): String {
    return this.metadata.name
}
