package org.springframework.data.mongodb.core.aggregation.operation;

import com.mongodb.DBObject;

/**
 * Represents one single operation in an aggregation pipeline
 *
 * @author Sebastian Herold
 * @since 1.3
 */
public interface AggregationOperation {

    /**
     * Gets the {@link DBObject} behind this operation
     *
     * @return the DBObject
     */
    DBObject getDBObject();
}
