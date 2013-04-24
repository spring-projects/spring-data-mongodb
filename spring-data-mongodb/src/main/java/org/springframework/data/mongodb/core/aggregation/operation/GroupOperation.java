package org.springframework.data.mongodb.core.aggregation.operation;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.springframework.data.mongodb.core.aggregation.ReferenceUtil;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Encapsulates the aggregation framework
 * <a href="http://docs.mongodb.org/manual/reference/aggregation/group/#stage._S_group">
 *     <code>$group</code>-operation
 * </a>
 *
 * @author Sebastian Herold
 * @since 1.3
 */
public class GroupOperation implements AggregationOperation {

    private static final String ID_KEY = "_id";
    private final Object id;
    private final Map<String, DBObject> fields = new HashMap<String, DBObject>();

    public GroupOperation(Object id) {
        this.id = id;
    }

    public DBObject getDBObject() {
        DBObject projection = new BasicDBObject(ID_KEY, id);
        for (Entry<String, DBObject> entry : fields.entrySet()) {
            projection.put(entry.getKey(), entry.getValue());
        }
        return new BasicDBObject("$group", projection);
    }

    public GroupOperation addField(String key, DBObject value) {
        Assert.hasText(key, "Key is empty");
        Assert.notNull(value, "Value is null");

        String trimmedKey = key.trim();
        if (ID_KEY.equals(trimmedKey)) {
            throw new IllegalArgumentException("_id field can only be set in constructor");
        }

        fields.put(key, value);

        return this;
    }

    /**
     * Adds a field with the
     * <a href="http://docs.mongodb.org/manual/reference/aggregation/addToSet/#grp._S_addToSet">$addToSet operation</a>.
     * <pre>
     *     {$group: {
     *          _id: "$id_field",
     *          name: {$addToSet: "$field"}
     *     }}
     * </pre>
     * @param name
     * @param field
     * @return
     *
     */
    public GroupOperation addToSet(String name, String field) {
        return addField(name, new BasicDBObject("$addToSet", ReferenceUtil.safeReference(field)));
    }

    /**
     * Creates a <code>$group</code> operation with <code>_id</code> referencing to a field of the document. The
     * returned db object equals to <pre>{_id: "$field"}</pre>
     * @param field
     * @return
     */
    public static GroupOperation group(String field) {
        return new GroupOperation(ReferenceUtil.safeReference(field));
    }

    /**
     * Creates a <code>$group</code> operation with a id that consists of multiple fields.
     *
     * Using {@link IdField#idField(String)} or {@link IdField#idField(String, String)} you can easily create
     * complex id fields like:
     * <pre>
     *
     *     group(idField("path"), idField("pageView", "page.views"), idField("field3"))
     *
     * </pre>
     * which would result in:
     * <pre>
     *
     *     {$group: {_id: {path: "$path", pageView: "$page.views", field3: "$field3"}}}
     *
     * </pre>
     * @param idFields
     * @return
     */
    public static GroupOperation group(IdField... idFields) {
        Assert.notNull(idFields, "Combined id is null");

        BasicDBObject id = new BasicDBObject();
        for (IdField idField : idFields) {
            id.put(idField.getKey(), idField.getValue());
        }

        return new GroupOperation(id);
    }

    /**
     * Represents a single field in a complex id of a <code>$group</code> operation.
     *
     * For example:
     * <pre>
     *     {$group: {_id: {key: "$value"}}}
     * </pre>
     */
    public static class IdField {

        private final String key;
        private final String value;

        public IdField(String key, String value) {
            Assert.hasText(key, "Key is empty");
            Assert.hasText(value, "Value is empty");

            this.key = ReferenceUtil.safeNonReference(key);
            this.value = ReferenceUtil.safeReference(value);
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        /**
         * Creates an id field with the name of the referenced field:
         * <pre>
         *     _id: {field: "$field"}
         * </pre>
         * @param field reference to a field of the document
         * @return the id field
         */
        public static IdField idField(String field) {
            return new IdField(field, field);
        }

        /**
         * Creates an id field with key and reference
         * <pre>
         *     _id: {key: "$field"}
         * </pre>
         * @param key the key
         * @param field reference to a field of the document
         * @return the id field
         */
        public static IdField idField(String key, String field) {
            return new IdField(key, field);
        }
    }
}
