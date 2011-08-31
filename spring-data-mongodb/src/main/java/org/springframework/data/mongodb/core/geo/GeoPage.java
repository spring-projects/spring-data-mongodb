/*
 * Copyright 2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *			http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.geo;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

/**
 * Custom {@link Page} to carry the average distance retrieved from the {@link GeoResults} the {@link GeoPage} is set up
 * from.
 * 
 * @author Oliver Gierke
 */
public class GeoPage<T> extends PageImpl<GeoResult<T>> {

	private static final long serialVersionUID = 23421312312412L;
	private final Distance averageDistance;

	/**
	 * Creates a new {@link GeoPage} from the given {@link GeoResults}.
	 * 
	 * @param content must not be {@literal null}.
	 */
	public GeoPage(GeoResults<T> results) {
		super(results.getContent());
		this.averageDistance = results.getAverageDistance();
	}

	/**
	 * Creates a new {@link GeoPage} from the given {@link GeoResults}, {@link Pageable} and total.
	 * 
	 * @param results must not be {@literal null}.
	 * @param pageable must not be {@literal null}.
	 * @param total
	 */
	public GeoPage(GeoResults<T> results, Pageable pageable, long total) {
		super(results.getContent(), pageable, total);
		this.averageDistance = results.getAverageDistance();
	}

	/**
	 * Returns the average distance of the underlying results.
	 * 
	 * @return the averageDistance
	 */
	public Distance getAverageDistance() {
		return averageDistance;
	}
}
