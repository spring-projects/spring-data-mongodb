/*
 * Copyright 2010 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;

import org.springframework.data.document.mongodb.CursorPreparer;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.domain.Sort.Order;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;


/**
 * Collection of utility methods to apply sorting and pagination to a
 * {@link DBCursor}.
 * 
 * @author Oliver Gierke
 */
abstract class MongoCursorUtils {
    
    private MongoCursorUtils() {
        
    }

    /**
     * Creates a {@link CursorPreparer} applying the given {@link Pageable} to
     * the cursor.
     * 
     * @param pageable
     * @return
     */
    public static CursorPreparer withPagination(Pageable pageable) {

        return new PaginationCursorPreparer(pageable);
    }


    /**
     * Creates a {@link CursorPreparer} to apply the given {@link Sort} to the
     * cursor.
     * 
     * @param sort
     * @return
     */
    public static CursorPreparer withSorting(Sort sort) {

        return new SortingCursorPreparer(sort);
    }

    /**
     * Applies the given {@link Pageable} to the given {@link DBCursor}.
     * 
     * @author Oliver Gierke
     */
    private static class PaginationCursorPreparer implements CursorPreparer {

        private final Pageable pageable;
        private final SortingCursorPreparer sortingPreparer;


        /**
         * Creates a new {@link PaginationCursorPreparer}.
         * 
         * @param pageable
         */
        public PaginationCursorPreparer(Pageable pageable) {

            this.pageable = pageable;
            this.sortingPreparer =
                    new SortingCursorPreparer(pageable.getSort());
        }


        /*
         * (non-Javadoc)
         * 
         * @see
         * org.springframework.data.document.mongodb.CursorPreparer#prepare(
         * com.mongodb.DBCursor)
         */
        public void prepare(DBCursor cursor) {

            if (pageable == null) {
                return;
            }

            int toSkip = pageable.getPageSize() * pageable.getPageNumber();
            int first = pageable.getPageSize();

            cursor.limit(first).skip(toSkip);
            sortingPreparer.prepare(cursor);
        }
    }

    /**
     * Applies the given {@link Sort} to the given {@link DBCursor}.
     * 
     * @author Oliver Gierke
     */
    private static class SortingCursorPreparer implements CursorPreparer {

        private final Sort sort;


        /**
         * Creates a new {@link SortingCursorPreparer}.
         * 
         * @param sort
         */
        public SortingCursorPreparer(Sort sort) {

            this.sort = sort;
        }


        /*
         * (non-Javadoc)
         * 
         * @see
         * org.springframework.data.document.mongodb.CursorPreparer#prepare(
         * com.mongodb.DBCursor)
         */
        public void prepare(DBCursor cursor) {

            if (sort == null) {
                return;
            }

            Map<String, Integer> sorts = new HashMap<String, Integer>();

            for (Order order : sort) {
                sorts.put(order.getProperty(),
                        Direction.ASC.equals(order.getDirection()) ? 1 : -1);
            }

            cursor.sort(new BasicDBObject(sorts));
        }
    }
}
