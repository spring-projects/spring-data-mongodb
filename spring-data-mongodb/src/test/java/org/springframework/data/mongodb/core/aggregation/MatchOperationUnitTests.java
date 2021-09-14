package org.springframework.data.mongodb.core.aggregation;


import static org.springframework.data.mongodb.test.util.Assertions.*;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Criteria;

/**
 * Unit tests for {@link MatchOperation}.
 *
 * @author Divya Srivastava
 */
class MatchOperationUnitTests {
	
	private static final String EXPRESSION_STRING = "{ $gt: [ \"$spent\" , \"$budget\" ] }";
	private static final Document EXPRESSION_DOC = Document.parse(EXPRESSION_STRING);
	private static final AggregationExpression EXPRESSION = context -> EXPRESSION_DOC;

	@Test // GH-3790
	void matchShouldRenderExpressionCorrectly() {

		MatchOperation operation = Aggregation.match(Criteria.expr(EXPRESSION));
		assertThat(operation.toDocument(Aggregation.DEFAULT_CONTEXT)).
				isEqualTo("{ $match:  { $expr : { $gt: [ \"$spent\" , \"$budget\" ] } } }");
	}
	
	@Test // GH-3790
	void matchShouldRenderCriteriaCorrectly() {

		MatchOperation operation = Aggregation.match(Criteria.where("spent").gt("$budget"));
		assertThat(operation.toDocument(Aggregation.DEFAULT_CONTEXT)).
				isEqualTo("{ $match:  { \"spent\" : { $gt : \"$budget\" } } }");
	}

}
