/*
 * Copyright 2010-2017 the original author or authors.
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
package org.springframework.data.mongodb.core;

import java.util.Optional;

import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.validation.ValidationOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Provides a simple wrapper to encapsulate the variety of settings you can use when creating a collection.
 *
 * @author Thomas Risberg
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Andreas Zink
 */
public class CollectionOptions {

	private @Nullable Long maxDocuments;
	private @Nullable Long size;
	private @Nullable Boolean capped;
	private @Nullable Collation collation;
	private @Nullable ValidationOptions validation;

	/**
	 * Constructs a new <code>CollectionOptions</code> instance.
	 *
	 * @param size the collection size in bytes, this data space is preallocated. Can be {@literal null}.
	 * @param maxDocuments the maximum number of documents in the collection. Can be {@literal null}.
	 * @param capped true to created a "capped" collection (fixed size with auto-FIFO behavior based on insertion order),
	 *          false otherwise. Can be {@literal null}.
	 * @deprecated since 2.0 please use {@link CollectionOptions#empty()} as entry point.
	 */
	@Deprecated
	public CollectionOptions(@Nullable Long size, @Nullable Long maxDocuments, @Nullable Boolean capped) {
		this(size, maxDocuments, capped, null, null);
	}

	private CollectionOptions(@Nullable Long size, @Nullable Long maxDocuments, @Nullable Boolean capped,
			@Nullable Collation collation, @Nullable ValidationOptions validation) {

		this.maxDocuments = maxDocuments;
		this.size = size;
		this.capped = capped;
		this.collation = collation;
		this.validation = validation;
	}

	/**
	 * Create new {@link CollectionOptions} by just providing the {@link Collation} to use.
	 *
	 * @param collation must not be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public static CollectionOptions just(Collation collation) {

		Assert.notNull(collation, "Collation must not be null!");

		return new CollectionOptions(null, null, null, collation, null);
	}

	/**
	 * Create new empty {@link CollectionOptions}.
	 *
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public static CollectionOptions empty() {
		return new CollectionOptions(null, null, null, null, null);
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and capped set to {@literal true}. <br />
	 * <strong>NOTE</strong> Using capped collections requires defining {@link #size(int)}.
	 *
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public CollectionOptions capped() {
		return new CollectionOptions(size, maxDocuments, true, collation, validation);
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code maxDocuments} set to given value.
	 *
	 * @param maxDocuments can be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public CollectionOptions maxDocuments(long maxDocuments) {
		return new CollectionOptions(size, maxDocuments, capped, collation, validation);
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code size} set to given value.
	 *
	 * @param size can be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public CollectionOptions size(long size) {
		return new CollectionOptions(size, maxDocuments, capped, collation, validation);
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code collation} set to given value.
	 *
	 * @param collation can be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public CollectionOptions collation(@Nullable Collation collation) {
		return new CollectionOptions(size, maxDocuments, capped, collation, validation);
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@link ValidationOptions} set to given value.
	 *
	 * @param validation can be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.1
	 */
	public CollectionOptions validation(@Nullable ValidationOptions validation) {
		return new CollectionOptions(size, maxDocuments, capped, collation, validation);
	}

	/**
	 * Get the max number of documents the collection should be limited to.
	 *
	 * @return {@link Optional#empty()} if not set.
	 */
	public Optional<Long> getMaxDocuments() {
		return Optional.ofNullable(maxDocuments);
	}

	/**
	 * Get the {@literal size} in bytes the collection should be limited to.
	 *
	 * @return {@link Optional#empty()} if not set.
	 */
	public Optional<Long> getSize() {
		return Optional.ofNullable(size);
	}

	/**
	 * Get if the collection should be capped.
	 *
	 * @return {@link Optional#empty()} if not set.
	 * @since 2.0
	 */
	public Optional<Boolean> getCapped() {
		return Optional.ofNullable(capped);
	}

	/**
	 * Get the {@link Collation} settings.
	 *
	 * @return {@link Optional#empty()} if not set.
	 * @since 2.0
	 */
	public Optional<Collation> getCollation() {
		return Optional.ofNullable(collation);
	}

	public Optional<ValidationOptions> getValidation() {
		return Optional.ofNullable(validation);
	}
}
