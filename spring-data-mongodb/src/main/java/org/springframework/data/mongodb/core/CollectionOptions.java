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

import org.springframework.util.Assert;

/**
 * Provides a simple wrapper to encapsulate the variety of settings you can use when creating a collection.
 *
 * @author Thomas Risberg
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class CollectionOptions {

	private Integer maxDocuments;
	private Integer size;
	private Boolean capped;
	private Optional<Collation> collation;

	/**
	 * Constructs a new <code>CollectionOptions</code> instance.
	 *
	 * @param size the collection size in bytes, this data space is preallocated.
	 * @param maxDocuments the maximum number of documents in the collection.
	 * @param capped true to created a "capped" collection (fixed size with auto-FIFO behavior based on insertion order),
	 *          false otherwise.
	 */
	public CollectionOptions(Integer size, Integer maxDocuments, Boolean capped) {
		this(size, maxDocuments, capped, Optional.empty());
	}

	private CollectionOptions(Integer size, Integer maxDocuments, Boolean capped, Optional<Collation> collation) {

		this.maxDocuments = maxDocuments;
		this.size = size;
		this.capped = capped;
		this.collation = collation;
	}

	private CollectionOptions() {}

	/**
	 * Create new {@link CollectionOptions} by just providing the {@link Collation} to use.
	 *
	 * @param collation must not be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public static CollectionOptions just(Collation collation) {

		Assert.notNull(collation, "Collation must not be null!");

		CollectionOptions options = new CollectionOptions();
		options.setCollation(collation);
		return options;
	}

	/**
	 * Create new empty {@link CollectionOptions}.
	 *
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public static CollectionOptions empty() {
		return new CollectionOptions();
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and capped set to {@literal true}.
	 *
	 * @param size the collection size in bytes, this data space is preallocated.
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public CollectionOptions capped(int size) {
		return new CollectionOptions(size, maxDocuments, true, collation);
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code maxDocuments} set to given value.
	 *
	 * @param maxDocuments can be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public CollectionOptions maxDocuments(Integer maxDocuments) {
		return new CollectionOptions(size, maxDocuments, capped, collation);
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code size} set to given value.
	 *
	 * @param size can be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public CollectionOptions size(int size) {
		return new CollectionOptions(size, maxDocuments, capped, collation);
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code collation} set to given value.
	 *
	 * @param collation can be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public CollectionOptions collation(Collation collation) {
		return new CollectionOptions(size, maxDocuments, capped, Optional.ofNullable(collation));
	}

	public Integer getMaxDocuments() {
		return maxDocuments;
	}

	public void setMaxDocuments(Integer maxDocuments) {
		this.maxDocuments = maxDocuments;
	}

	public Integer getSize() {
		return size;
	}

	public void setSize(Integer size) {
		this.size = size;
	}

	public Boolean getCapped() {
		return capped;
	}

	public void setCapped(Boolean capped) {
		this.capped = capped;
	}

	/**
	 * Set {@link Collation} options.
	 *
	 * @param collation
	 * @since 2.0
	 */
	public void setCollation(Collation collation) {
		this.collation = Optional.ofNullable(collation);
	}

	/**
	 * Get the {@link Collation} settings.
	 *
	 * @return
	 * @since 2.0
	 */
	public Optional<Collation> getCollation() {
		return collation;
	}
}
