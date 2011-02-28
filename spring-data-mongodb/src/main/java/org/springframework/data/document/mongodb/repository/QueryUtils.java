/*
 * Copyright 2010-2011 the original author or authors.
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
package org.springframework.data.document.mongodb.repository;

import org.springframework.data.document.mongodb.query.Query;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;

import com.mongodb.DBCursor;


/**
 * Collection of utility methods to apply sorting and pagination to a
 * {@link DBCursor}.
 * 
 * @author Oliver Gierke
 */
abstract class QueryUtils {

    private QueryUtils() {

    }


    /**
     * Applies the given {@link Pageable} to the given {@link Query}. Will do
     * nothing if {@link Pageable} is {@literal null}.
     * 
     * @param query
     * @param pageable
     * @return
     */
    public static Query applyPagination(Query query, Pageable pageable) {

        if (pageable == null) {
            return query;
        }

        query.limit(pageable.getPageSize());
        query.skip(pageable.getOffset());

        return applySorting(query, pageable.getSort());
    }


    /**
     * Applies the given {@link Sort} to the {@link Query}. Will do nothing if
     * {@link Sort} is {@literal null}.
     * 
     * @param query
     * @param sort
     * @return
     */
    public static Query applySorting(Query query, Sort sort) {

        if (sort == null) {
            return query;
        }

        org.springframework.data.document.mongodb.query.Sort bSort =
                query.sort();

        for (Order order : sort) {
            bSort.on(
                    order.getProperty(),
                    order.isAscending() ? org.springframework.data.document.mongodb.query.Order.ASCENDING
                            : org.springframework.data.document.mongodb.query.Order.DESCENDING);
        }

        return query;
    }
}
