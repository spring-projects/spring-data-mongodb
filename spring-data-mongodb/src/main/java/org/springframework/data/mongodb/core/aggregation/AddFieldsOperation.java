package org.springframework.data.mongodb.core.aggregation;

import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;

import java.util.LinkedHashMap;
import java.util.Map;

public class AddFieldsOperation implements AggregationOperation {

    private Map<String, Object> fields = new LinkedHashMap<>();

    public AddFieldsOperation (String field, Object value) {
        fields.put(field, value);
    }

    @Override
    public Document toDocument(AggregationOperationContext context) {
        Document doc = new Document();
        fields.forEach(doc::append);

        return new Document("$addFields", doc);
    }

    public AddFieldsOperation addField(String field, Object value) {
        this.fields.put(field, value);
        return this;
    }
}
 
