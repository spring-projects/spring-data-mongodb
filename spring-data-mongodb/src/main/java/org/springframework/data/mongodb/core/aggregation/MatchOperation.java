package org.springframework.data.mongodb.core.aggregation.operation;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.springframework.data.mongodb.core.query.Criteria;

/**
 * Encapsulates the <code>$match</code>-operation
 *
 * @author Sebastian Herold
 * @since 1.3
 */
public class MatchOperation implements AggregationOperation {

    private final DBObject criteria;

    public MatchOperation(Criteria criteria) {
        this.criteria = criteria.getCriteriaObject();
    }

    public DBObject getDBObject() {
        return new BasicDBObject("$match", criteria);
    }

    public static MatchOperation match(Criteria criteria) {
        return new MatchOperation(criteria);
    }
}
