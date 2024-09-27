/*
 * Copyright 2024 the original author or authors.
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
@file:Suppress("INVISIBLE_MEMBER", "INVISIBLE_REFERENCE")
package org.springframework.data.mongodb.core.query

import org.springframework.data.mapping.toDotPath
import org.springframework.data.mongodb.core.query.Update.Position
import kotlin.reflect.KProperty

/**
 * Static factory method to create an Update using the provided key
 *
 * @author Pawel Matysek
 * @since 4.4
 * @see Update.update
 */
fun <@kotlin.internal.OnlyInputTypes T> update(key: KProperty<T>, value: T?) =
    Update.update(key.toDotPath(), value)

/**
 * Update using the {@literal $set} update modifier
 *
 * @author Pawel Matysek
 * @since 4.4
 * @see Update.set
 */
fun <@kotlin.internal.OnlyInputTypes T> Update.set(key: KProperty<T>, value: T?) =
    set(key.toDotPath(), value)

/**
 * Update using the {@literal $setOnInsert} update modifier
 *
 * @author Pawel Matysek
 * @since 4.4
 * @see Update.setOnInsert
 */
fun <@kotlin.internal.OnlyInputTypes T> Update.setOnInsert(key: KProperty<T>, value: T?) =
    setOnInsert(key.toDotPath(), value)

/**
 * Update using the {@literal $unset} update modifier
 *
 * @author Pawel Matysek
 * @since 4.4
 * @see Update.unset
 */
fun <@kotlin.internal.OnlyInputTypes T> Update.unset(key: KProperty<T>) =
    unset(key.toDotPath())

/**
 * Update using the {@literal $inc} update modifier
 *
 * @author Pawel Matysek
 * @since 4.4
 * @see Update.inc
 */
fun <@kotlin.internal.OnlyInputTypes T> Update.inc(key: KProperty<T>, inc: Number) =
    inc(key.toDotPath(), inc)

fun <@kotlin.internal.OnlyInputTypes T> Update.inc(key: KProperty<T>) =
    inc(key.toDotPath())

/**
 * Update using the {@literal $push} update modifier
 *
 * @author Pawel Matysek
 * @since 4.4
 * @see Update.push
 */
fun <@kotlin.internal.OnlyInputTypes T> Update.push(key: KProperty<Collection<T>>, value: T?) =
    push(key.toDotPath(), value)

/**
 * Update using {@code $push} modifier. <br/>
 * Allows creation of {@code $push} command for single or multiple (using {@code $each}) values as well as using
 * {@code $position}.
 *
 * @author Pawel Matysek
 * @since 4.4
 * @see Update.push
 */
fun <@kotlin.internal.OnlyInputTypes T> Update.push(key: KProperty<T>) =
    push(key.toDotPath())

/**
 * Update using {@code $addToSet} modifier. <br/>
 * Allows creation of {@code $push} command for single or multiple (using {@code $each}) values * {@code $position}.
 *
 * @author Pawel Matysek
 * @since 4.4
 * @see Update.addToSet
 */
fun <@kotlin.internal.OnlyInputTypes T> Update.addToSet(key: KProperty<T>) =
    addToSet(key.toDotPath())

/**
 * Update using the {@literal $addToSet} update modifier
 *
 * @author Pawel Matysek
 * @since 4.4
 * @see Update.addToSet
 */
fun <@kotlin.internal.OnlyInputTypes T> Update.addToSet(key: KProperty<Collection<T>>, value: T?) =
    addToSet(key.toDotPath(), value)

/**
 * Update using the {@literal $pop} update modifier
 *
 * @author Pawel Matysek
 * @since 4.4
 * @see Update.pop
 */
fun <@kotlin.internal.OnlyInputTypes T> Update.pop(key: KProperty<T>, pos: Position) =
    pop(key.toDotPath(), pos)

/**
 * Update using the {@literal $pull} update modifier
 *
 * @author Pawel Matysek
 * @since 4.4
 * @see Update.pull
 */
fun <@kotlin.internal.OnlyInputTypes T> Update.pull(key: KProperty<T>, value: Any) =
    pull(key.toDotPath(), value)

/**
 * Update using the {@literal $pullAll} update modifier
 *
 * @author Pawel Matysek
 * @since 4.4
 * @see Update.pullAll
 */
fun <@kotlin.internal.OnlyInputTypes T> Update.pullAll(key: KProperty<Collection<T>>, values: Array<T>) =
    pullAll(key.toDotPath(), values)

/**
 * Update given key to current date using {@literal $currentDate} modifier.
 *
 * @author Pawel Matysek
 * @since 4.4
 * @see Update.currentDate
 */
fun <@kotlin.internal.OnlyInputTypes T> Update.currentDate(key: KProperty<T>) =
    currentDate(key.toDotPath())

/**
 * Update given key to current date using {@literal $currentDate : &#123; $type : "timestamp" &#125;} modifier.
 *
 * @author Pawel Matysek
 * @since 4.4
 * @see Update.currentTimestamp
 */
fun <@kotlin.internal.OnlyInputTypes T> Update.currentTimestamp(key: KProperty<T>) =
    currentTimestamp(key.toDotPath())

/**
 * Multiply the value of given key by the given number.
 *
 * @author Pawel Matysek
 * @since 4.4
 * @see Update.multiply
 */
fun <@kotlin.internal.OnlyInputTypes T> Update.multiply(key: KProperty<T>, multiplier: Number) =
    multiply(key.toDotPath(), multiplier)

/**
 * Update given key to the {@code value} if the {@code value} is greater than the current value of the field.
 *
 * @author Pawel Matysek
 * @since 4.4
 * @see Update.max
 */
fun <T : Any> Update.max(key: KProperty<T>, value: T) =
    max(key.toDotPath(), value)

/**
 * Update given key to the {@code value} if the {@code value} is less than the current value of the field.
 *
 * @author Pawel Matysek
 * @since 4.4
 * @see Update.min
 */
fun <T : Any> Update.min(key: KProperty<T>, value: T) =
    min(key.toDotPath(), value)

/**
 * The operator supports bitwise {@code and}, bitwise {@code or}, and bitwise {@code xor} operations.
 *
 * @author Pawel Matysek
 * @since 4.4
 * @see Update.bitwise
 */
fun <@kotlin.internal.OnlyInputTypes T> Update.bitwise(key: KProperty<T>) =
    bitwise(key.toDotPath())

/**
 * Filter elements in an array that match the given criteria for update. {@code expression} is used directly with the
 * driver without further type or field mapping.
 *
 * @author Pawel Matysek
 * @since 4.4
 * @see Update.filterArray
 */
fun <@kotlin.internal.OnlyInputTypes T> Update.filterArray(identifier: KProperty<T>, expression: Any) =
    filterArray(identifier.toDotPath(), expression)

/**
 * Determine if a given {@code key} will be touched on execution.
 *
 * @author Pawel Matysek
 * @since 4.4
 * @see Update.modifies
 */
fun <@kotlin.internal.OnlyInputTypes T> Update.modifies(key: KProperty<T>) =
    modifies(key.toDotPath())

