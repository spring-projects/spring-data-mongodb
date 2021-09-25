package org.springframework.data.mongodb.core.aggregation;

import org.bson.Document;
import org.springframework.util.Assert;

/**
 * Encapsulates the {@code search}-operation.
 *
 * @see <a href=
 *      "https://docs.mongodb.com/manual/reference/operator/aggregation/search/">https://docs.mongodb.com/manual/reference/operator/aggregation/search/</a>
 */
public class SearchOperation implements AggregationOperation {

	private static final int DEFAULT_MAXCHARSTOEXAMINE = 500000;
	private static final int DEFAULT_MAXNUMPASSAGES = 5;

	private final String indexName;
	private final SearchOperator searchOperator;
	private final HighlightOption highlights;

	/**
	 * Create a new {@link SearchOperation} with given arguments.
	 *
	 * @param indexName Name of the Atlas Search index to use.
	 * @param searchOperator the operator to search with.
	 * @param highlights Document that specifies the Highlighting options for displaying search terms in their original
	 *          context.
	 */
	protected SearchOperation(String indexName, SearchOperator searchOperator, HighlightOption highlights) {
		this.indexName = indexName;
		this.searchOperator = searchOperator;
		this.highlights = highlights;
	}

	/**
	 * Obtain a {@link SearchOperationBuilder builder} to create a {@link SearchOperation}.
	 *
	 * @return new instance of {@link SearchOperationBuilder}.
	 */
	public static SearchOperationBuilder builder() {
		return new SearchOperationBuilder();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#
	 * getOperator()
	 */
	@Override
	public String getOperator() {
		return "$search";
	}

	@Override
	public Document toDocument(AggregationOperationContext context) {
		Document doc = new Document();
		if (highlights != null) {
			doc.append("highlights", highlights.toDocument());
		}
		if (searchOperator != null) {
			doc.append(searchOperator.operator.getMongoOperator(), searchOperator.options);
		}
		doc.append("index", indexName != null ? indexName : "default");
		return new Document(getOperator(), doc);
	}

	/**
	 * A builder to create a {@link SearchOperation}.
	 */
	public static class SearchOperationBuilder {

		private SearchOperator operator;
		private String indexName;
		private HighlightOption highlights;

		/**
		 * Specify the name of the index to use
		 *
		 * @param indexName must not be {@literal null} or null.
		 * @return this.
		 */
		public SearchOperationBuilder withIndex(String indexName) {
			Assert.hasText(indexName, "Index name must not be empty or null");
			this.indexName = indexName;
			return this;
		}

		/**
		 * Specify the document with the options of the (@link Operators} to search with.
		 *
		 * @param options must not be {@literal null}
		 * @return this.
		 */
		public SearchOperationBuilder withOptions(Document options) {
			Assert.notNull(options, "Options must not be null");
			this.operator.options = options;
			return this;
		}

		/**
		 * Specify the fields for the {@link HighlightOption} option
		 *
		 * @param path must not be {@literal null}
		 * @return this.
		 */
		public SearchOperationBuilder withHighlight(String path) {
			Assert.notNull(path, "Path must not be null");
			this.highlights = new HighlightOption(path);
			return this;
		}

		/**
		 * Specify the fields for the {@link HighlightOption} option
		 *
		 * @param path must not be {@literal null}
		 * @param maxCharsToExamine Maximum number of characters to examine on a document when performing highlighting for a
		 *          field.
		 * @return this.
		 */
		public SearchOperationBuilder withHighlight(String path, int maxCharsToExamine) {
			Assert.notNull(path, "Path must not be null");
			this.highlights = new HighlightOption(path, maxCharsToExamine);
			return this;
		}

		/**
		 * Specify the fields for the {@link HighlightOption} option
		 *
		 * @param path must not be {@literal null}
		 * @param maxCharsToExamine Maximum number of characters to examine on a document when performing highlighting for a
		 *          field.
		 * @param maxNumPassages Number of high-scoring passages to return per document in the highlights results for each
		 *          field.
		 * @return this.
		 */
		public SearchOperationBuilder withHighlight(String path, int maxCharsToExamine, int maxNumPassages) {
			Assert.notNull(path, "Path must not be null");
			this.highlights = new HighlightOption(path, maxCharsToExamine, maxNumPassages);
			return this;
		}

		/**
		 * Performs a search-as-you-type query from an incomplete input string.
		 * 
		 * @return this
		 */
		public SearchOperationBuilder autocomplete() {
			this.operator = new SearchOperator(Operators.AUTOCOMPLETE, null);
			return this;
		}

		/**
		 * Combines other operators into a single query.
		 * 
		 * @return this
		 */
		public SearchOperationBuilder compound() {
			this.operator = new SearchOperator(Operators.COMPOUND, null);
			return this;
		}

		/**
		 * Works in conjunction with the boolean and objectId data types.
		 * 
		 * @return
		 */
		public SearchOperationBuilder equals() {
			this.operator = new SearchOperator(Operators.EQUALS, null);
			return this;
		}

		/**
		 * Tests for the presence of a specified field.
		 * 
		 * @return this
		 */
		public SearchOperationBuilder exists() {
			this.operator = new SearchOperator(Operators.EXISTS, null);
			return this;
		}

		/**
		 * Queries for values with specified geo shapes.
		 * 
		 * @return this
		 */
		public SearchOperationBuilder geoShape() {
			this.operator = new SearchOperator(Operators.GEOSHAPE, null);
			return this;
		}

		/**
		 * Queries for points within specified geographic shapes.
		 * 
		 * @return this
		 */
		public SearchOperationBuilder geoWithin() {
			this.operator = new SearchOperator(Operators.GEOWITHIN, null);
			return this;
		}

		/**
		 * Queries for values near a specified number, date, or geo point.
		 * 
		 * @return this
		 */
		public SearchOperationBuilder near() {
			this.operator = new SearchOperator(Operators.NEAR, null);
			return this;
		}

		/**
		 * Searches documents for terms in an order similar to the query.
		 * 
		 * @return this
		 */
		public SearchOperationBuilder phrase() {
			this.operator = new SearchOperator(Operators.PHRASE, null);
			return this;
		}

		/**
		 * Supports querying a combination of indexed fields and values.
		 * 
		 * @return this
		 */
		public SearchOperationBuilder queryString() {
			this.operator = new SearchOperator(Operators.QUERYSTRING, null);
			return this;
		}

		/**
		 * Queries for values within a specific numeric or date range.
		 * 
		 * @return this
		 */
		public SearchOperationBuilder range() {
			this.operator = new SearchOperator(Operators.RANGE, null);
			return this;
		}

		/**
		 * Interprets the query field as a regular expression.
		 * 
		 * @return this
		 */
		public SearchOperationBuilder regex() {
			this.operator = new SearchOperator(Operators.REGEX, null);
			return this;
		}

		/**
		 * @deprecated Performs analyzed search. Use text operator instead.
		 * @return this
		 */
		public SearchOperationBuilder search() {
			this.operator = new SearchOperator(Operators.SEARCH, null);
			return this;
		}

		/**
		 * Specifies relative positional requirements for query predicates within specified regions of a text field.
		 * 
		 * @return this
		 */
		public SearchOperationBuilder span() {
			this.operator = new SearchOperator(Operators.SPAN, null);
			return this;
		}

		/**
		 * @deprecated Performs unanalyzed search.
		 * @return this
		 */
		public SearchOperationBuilder term() {
			this.operator = new SearchOperator(Operators.TERM, null);
			return this;
		}

		/**
		 * Performs textual analyzed search.
		 * 
		 * @return this
		 */
		public SearchOperationBuilder text() {
			this.operator = new SearchOperator(Operators.TEXT, null);
			return this;
		}

		/**
		 * Supports special characters in the query string that can match any character.
		 * 
		 * @return this
		 */
		public SearchOperationBuilder wildcard() {
			this.operator = new SearchOperator(Operators.WILDCARD, null);
			return this;
		}

		/**
		 * Obtain a new instance of {@link SearchOperation} with previously set arguments.
		 *
		 * @return new instance of {@link SearchOperation}.
		 */
		public SearchOperation build() {
			return new SearchOperation(indexName, operator, highlights);
		}

	}

	/**
	 * The operators supported by the search operation to search with.
	 */
	private static enum Operators {
		
		AUTOCOMPLETE("$autocomplete"), COMPOUND("$compound"), EQUALS("$equals"), EXISTS("$exists"), 
		GEOSHAPE("$geoShape"), GEOWITHIN("$geoWithin"), NEAR("$near"), PHRASE("$phrase"), QUERYSTRING("$queryString"), 
		RANGE("$range"), REGEX("$regex"), SEARCH("$search"), SPAN("$span"), TERM("$term"), TEXT("$text"), WILDCARD("$wildcard");

		private String mongoOperator;

		Operators(String mongoOperator) {
			this.mongoOperator = mongoOperator;
		}

		public String getMongoOperator() {
			return mongoOperator;
		}

	}

	/**
	 * A structure capturing the {@link Operators} and the {@link Document} that contains the operator-specific options
	 */
	public static class SearchOperator {

		private Operators operator;
		private Document options;

		/**
		 * Creates a new SearchOperator with the {@link Operators} and the {@link Document} with the options
		 * 
		 * @param operator the {@link Operators} to search with.
		 * @param options {@link Document} with the operator options
		 */
		public SearchOperator(Operators operator, Document options) {
			this.operator = operator;
			this.options = options;
		}
	}

	/**
	 * A structure specifying the the highlighting options for displaying search terms in their original context.
	 */
	public static class HighlightOption {

		private String path;
		private int maxCharsToExamine;
		private int maxNumPassages;

		/**
		 * Create a new {@link HighlightOption}.
		 *
		 * @param path Document field to search.
		 */
		public HighlightOption(String path) {
			this(path, DEFAULT_MAXCHARSTOEXAMINE, DEFAULT_MAXNUMPASSAGES);
		}

		/**
		 * Create a new {@link HighlightOption}.
		 *
		 * @param path Document field to search.
		 * @param maxCharsToExamine Maximum number of characters to examine on a document when performing highlighting for a
		 *          field.
		 */
		public HighlightOption(String path, int maxCharsToExamine) {
			this(path, maxCharsToExamine, DEFAULT_MAXNUMPASSAGES);
		}

		/**
		 * Create a new {@link HighlightOption}.
		 *
		 * @param path Document field to search.
		 * @param maxCharsToExamine Maximum number of characters to examine on a document when performing highlighting for a
		 *          field.
		 * @param maxNumPassages Number of high-scoring passages to return per document in the highlights results for each
		 *          field.
		 */
		public HighlightOption(String path, int maxCharsToExamine, int maxNumPassages) {
			this.path = path;
			this.maxCharsToExamine = maxCharsToExamine;
			this.maxNumPassages = maxNumPassages;
		}

		public Document toDocument() {
			return new Document().append("path", path).append("maxCharsToExamine", maxCharsToExamine).append("maxNumPassages",
					maxNumPassages);
		}

		public Object getPath() {
			return path;
		}

		public int getMaxCharsToExamine() {
			return maxCharsToExamine;
		}

		public int getMaxNumPassages() {
			return maxNumPassages;
		}
	}

}
