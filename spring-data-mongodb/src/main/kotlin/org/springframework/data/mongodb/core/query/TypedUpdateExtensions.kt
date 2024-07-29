/*
 * Copyright 2018-2024 the original author or authors.
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

import org.springframework.data.mapping.toDotPath
import org.springframework.data.mongodb.core.query.Update.Position
import kotlin.reflect.KProperty

/**
 * Static factory method to create an Update using the provided key
 *
 * @author Pawel Matysek
 * @see Update.update
 */
fun <T> update(key: KProperty<T>, value: T?) =
    Update.update(key.toDotPath(), value)

/**
 * Update using the {@literal $set} update modifier
 *
 * @author Pawel Matysek
 * @see Update.set
 */
fun <T> Update.set(key: KProperty<T>, value: T?) =
    set(key.toDotPath(), value)

/**
 * Update using the {@literal $setOnInsert} update modifier
 *
 * @author Pawel Matysek
 * @see Update.setOnInsert
 */
fun <T> Update.setOnInsert(key: KProperty<T>, value: T?) =
    setOnInsert(key.toDotPath(), value)

/**
 * Update using the {@literal $unset} update modifier
 *
 * @author Pawel Matysek
 * @see Update.unset
 */
fun <T> Update.unset(key: KProperty<T>) =
    unset(key.toDotPath())

/**
 * Update using the {@literal $inc} update modifier
 *
 * @author Pawel Matysek
 * @see Update.inc
 */
fun <T> Update.inc(key: KProperty<T>, inc: Number) =
    inc(key.toDotPath(), inc)

fun <T> Update.inc(key: KProperty<T>) =
    inc(key.toDotPath())

/**
 * Update using the {@literal $push} update modifier
 *
 * @author Pawel Matysek
 * @see Update.push
 */
fun <T> Update.push(key: KProperty<Collection<T>>, value: T?) =
    push(key.toDotPath(), value)

/**
 * Update using {@code $push} modifier. <br/>
 * Allows creation of {@code $push} command for single or multiple (using {@code $each}) values as well as using
 *
 * {@code $position}.
 * @author Pawel Matysek
 * @see Update.push
 */
fun <T> Update.push(key: KProperty<T>) =
    push(key.toDotPath())

/**
 * Update using {@code $addToSet} modifier. <br/>
 * Allows creation of {@code $push} command for single or multiple (using {@code $each}) values * {@code $position}.
 *
 * @author Pawel Matysek
 * @see Update.addToSet
 */
fun <T> Update.addToSet(key: KProperty<T>) =
    addToSet(key.toDotPath())

/**
 * Update using the {@literal $addToSet} update modifier
 *
 * @author Pawel Matysek
 * @see Update.addToSet
 */
fun <T> Update.addToSet(key: KProperty<Collection<T>>, value: T?) =
    addToSet(key.toDotPath(), value)

/**
 * Update using the {@literal $pop} update modifier
 *
 * @author Pawel Matysek
 * @see Update.pop
 */
fun <T> Update.pop(key: KProperty<T>, pos: Position) =
    pop(key.toDotPath(), pos)

/**
 * Update using the {@literal $pull} update modifier
 *
 * @author Pawel Matysek
 * @see Update.pull
 */
fun <T> Update.pull(key: KProperty<T>, value: Any) =
    pull(key.toDotPath(), value)

/**
 * Update using the {@literal $pullAll} update modifier
 *
 * @author Pawel Matysek
 * @see Update.pullAll
 */
fun <T> Update.pullAll(key: KProperty<Collection<T>>, values: Array<T>) =
    pullAll(key.toDotPath(), values)

/**
 * Update given key to current date using {@literal $currentDate : &#123; $type : "timestamp" &#125;} modifier.
 *
 * @author Pawel Matysek
 * @see Update.currentDate
 */
fun <T> Update.currentDate(key: KProperty<T>) =
    currentDate(key.toDotPath())

/**
 * Update given key to current date using {@literal $currentDate} modifier.
 *
 * @author Pawel Matysek
 * @see Update.currentTimestamp
 */
fun <T> Update.currentTimestamp(key: KProperty<T>) =
    currentTimestamp(key.toDotPath())

/**
 * Multiply the value of given key by the given number.
 *
 * @author Pawel Matysek
 * @see Update.multiply
 */
fun <T> Update.multiply(key: KProperty<T>, multiplier: Number) =
    multiply(key.toDotPath(), multiplier)

/**
 * Update given key to the {@code value} if the {@code value} is greater than the current value of the field.
 *
 * @author Pawel Matysek
 * @see Update.max
 */
fun <T : Any> Update.max(key: KProperty<T>, value: T) =
    max(key.toDotPath(), value)

/**
 * Update given key to the {@code value} if the {@code value} is less than the current value of the field.
 *
 * @author Pawel Matysek
 * @see Update.min
 */
fun <T : Any> Update.min(key: KProperty<T>, value: T) =
    min(key.toDotPath(), value)

/**
 * The operator supports bitwise {@code and}, bitwise {@code or}, and bitwise {@code xor} operations.
 *
 * @author Pawel Matysek
 * @see Update.bitwise
 */
fun <T> Update.bitwise(key: KProperty<T>) =
    bitwise(key.toDotPath())

/**
 * Filter elements in an array that match the given criteria for update. {@code expression} is used directly with the
 * driver without further type or field mapping.
 *
 * @author Pawel Matysek
 * @see Update.filterArray
 */
fun <T> Update.filterArray(identifier: KProperty<T>, expression: Any) =
    filterArray(identifier.toDotPath(), expression)

/**
 * Determine if a given {@code key} will be touched on execution.
 *
 * @author Pawel Matysek
 * @see Update.modifies
 */
fun <T> Update.modifies(key: KProperty<T>) =
    modifies(key.toDotPath())

