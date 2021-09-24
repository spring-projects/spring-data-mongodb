package org.springframework.data.mongodb.core.aggregation;

import java.util.Map;

import org.bson.Document;
import org.springframework.util.Assert;

public class SearchOperation implements AggregationOperation {

	private static final int DEFAULT_MAXCHARSTOEXAMINE = 500000;
	private static final int DEFAULT_MAXNUMPASSAGES = 5;

	private final String indexName;
	private final SearchOperator searchOperator;
	private final HighlightOption highlights;

	protected SearchOperation(String indexName, SearchOperator searchOperator, HighlightOption highlights) {
		this.indexName = indexName;
		this.searchOperator = searchOperator;
		this.highlights = highlights;
	}

	public static SearchOperationBuilder builder() {
		return new SearchOperationBuilder();
	}

	public static class SearchOperationBuilder {

		private SearchOperator operator;
		private String indexName;
		private HighlightOption highlights;

		public SearchOperationBuilder autocomplete() {
			this.operator = new SearchOperator(Operators.AUTOCOMPLETE, null);
			return this;
		}

		public SearchOperationBuilder withOptions(Map<String, Object> options) {
			Assert.notEmpty(options, "Options By must not be null");
			this.operator.options = options;
			return this;
		}

		public SearchOperationBuilder withIndex(String indexName) {
			Assert.notNull(indexName, "Partition By must not be null");
			this.indexName = indexName;
			return this;
		}

		public SearchOperationBuilder withHighlight(String path) {
			Assert.notNull(path, "Path must not be null");
			this.highlights = new HighlightOption(path);
			return this;
		}

		public SearchOperationBuilder withHighlight(String path, int maxCharsToExamine) {
			Assert.notNull(path, "Path must not be null");
			this.highlights = new HighlightOption(path, maxCharsToExamine);
			return this;
		}

		public SearchOperationBuilder withHighlight(String path, int maxCharsToExamine, int maxNumPassages) {
			Assert.notNull(path, "Path must not be null");
			this.highlights = new HighlightOption(path, maxCharsToExamine, maxNumPassages);
			return this;
		}

		public SearchOperation build() {
			return new SearchOperation(indexName, operator, highlights);
		}
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
		return new Document(getMongoOperator(),doc);
	}

	public String getMongoOperator() {
		return "$search";
	}

	private enum Operators {
		AUTOCOMPLETE("$autocomplete"), COMPOUND("$compound"), EQUALS("$equals"), EXISTS("$exists"),
		GEOSHAPE("$geoShape"), GEOWITHIN("$geoWithin"), NEAR("$near"), PHRASE("$phrase"), QUERYSTRING("$queryString"),
		RANGE("$range"), REGEX("$regex"), SEARCH("$search"), SPAN("$span"), TERM("$term"), TEXT("$text"),
		WILDCARD("$wildcard");

		private String mongoOperator;

		Operators(String mongoOperator) {
			this.mongoOperator = mongoOperator;
		}

		public String getMongoOperator() {
			return mongoOperator;
		}

	}

	public static class SearchOperator {

		private Operators operator;
		private Map<String, Object> options;

		public SearchOperator(Operators operator, Map<String, Object> options) {
			this.operator = operator;
			this.options = options;
		}
	}

	public static class HighlightOption {

		private String path;
		private int maxCharsToExamine;
		private int maxNumPassages;

		public HighlightOption(String path) {
			this(path, DEFAULT_MAXCHARSTOEXAMINE, DEFAULT_MAXNUMPASSAGES);
		}

		public HighlightOption(String path, int maxCharsToExamine) {
			this(path, maxCharsToExamine, DEFAULT_MAXNUMPASSAGES);
		}

		public HighlightOption(String path, int maxCharsToExamine, int maxNumPassages) {
			this.path = path;
			this.maxCharsToExamine = maxCharsToExamine;
			this.maxNumPassages = maxNumPassages;
		}

		public Document toDocument() {
			return new Document().append("path", path).append("maxCharsToExamine", maxCharsToExamine)
					.append("maxNumPassages", maxNumPassages);
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
