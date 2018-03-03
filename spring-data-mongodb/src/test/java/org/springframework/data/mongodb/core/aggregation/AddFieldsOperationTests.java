package org.springframework.data.mongodb.core.aggregation;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.DocumentTestUtils.*;

import org.bson.Document;
import org.junit.Test;

/**
 * Unit tests for {@link AddFieldsOperation}.
 *
 * @author Vadzim Parafianiuk
 */
public class AddFieldsOperationTests {

    @Test
    public void createDocumentForAddFieldsWithInnerDocumentCorrectly() {
        AddFieldsOperation operation = new AddFieldsOperation("foo", new Document("$size", "$someArray"));
        Document result = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

        Document addFieldsValue = getAsDocument(result, "$addFields");
        assertThat(addFieldsValue, is(notNullValue()));
        assertThat(addFieldsValue.get("foo"), is(new Document("$size", "$someArray")));
    }

    @Test
    public void createDocumentForAddFieldsCorrectly() {
        AddFieldsOperation operation = new AddFieldsOperation("foo", "bar");
        Document result = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

        Document addFieldsValue = getAsDocument(result, "$addFields");
        assertThat(addFieldsValue, is(notNullValue()));
        assertThat(addFieldsValue.get("foo"), is("bar"));
    }

    @Test
    public void createMultiFieldAddFieldsOperation() {
        AddFieldsOperation operation =
                new AddFieldsOperation("foo", "bar")
                .addField("counter", new Document("$size", "$arrayWithPicturesWithCats"));

        Document result = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

        Document addFieldsValue = getAsDocument(result, "$addFields");
        assertThat(addFieldsValue.get("counter"), is(new Document("$size", "$arrayWithPicturesWithCats")));
        assertThat(addFieldsValue.get("foo"), is("bar"));
    }


}
