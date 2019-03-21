/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.util.json;

import static org.assertj.core.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.junit.Test;

/**
 * @author Christoph Strobl
 */
public class ParameterBindingJsonReaderUnitTests {

	@Test
	public void bindUnquotedStringValue() {

		Document target = parse("{ 'lastname' : ?0 }", "kohlin");
		assertThat(target).isEqualTo(new Document("lastname", "kohlin"));
	}

	@Test
	public void bindQuotedStringValue() {

		Document target = parse("{ 'lastname' : '?0' }", "kohlin");
		assertThat(target).isEqualTo(new Document("lastname", "kohlin"));
	}

	@Test
	public void bindUnquotedIntegerValue() {

		Document target = parse("{ 'lastname' : ?0 } ", 100);
		assertThat(target).isEqualTo(new Document("lastname", 100));
	}

	@Test
	public void bindMultiplePlacholders() {

		Document target = parse("{ 'lastname' : ?0, 'firstname' : '?1' }", "Kohlin", "Dalinar");
		assertThat(target).isEqualTo(Document.parse("{ 'lastname' : 'Kohlin', 'firstname' : 'Dalinar' }"));
	}

	@Test
	public void bindQuotedIntegerValue() {

		Document target = parse("{ 'lastname' : '?0' }", 100);
		assertThat(target).isEqualTo(new Document("lastname", "100"));
	}

	@Test
	public void bindValueToRegex() {

		Document target = parse("{ 'lastname' : { '$regex' : '^(?0)'} }", "kohlin");
		assertThat(target).isEqualTo(Document.parse("{ 'lastname' : { '$regex' : '^(kohlin)'} }"));
	}

	@Test
	public void bindValueToMultiRegex() {

		Document target = parse(
				"{'$or' : [{'firstname': {'$regex': '.*?0.*', '$options': 'i'}}, {'lastname' : {'$regex': '.*?0xyz.*', '$options': 'i'}} ]}",
				"calamity");
		assertThat(target).isEqualTo(Document.parse(
				"{ \"$or\" : [ { \"firstname\" : { \"$regex\" : \".*calamity.*\" , \"$options\" : \"i\"}} , { \"lastname\" : { \"$regex\" : \".*calamityxyz.*\" , \"$options\" : \"i\"}}]}"));
	}

	@Test
	public void bindMultipleValuesToSingleToken() {

		Document target = parse("{$where: 'return this.date.getUTCMonth() == ?2 && this.date.getUTCDay() == ?3;'}", 0, 1, 2,
				3, 4);
		assertThat(target)
				.isEqualTo(Document.parse("{$where: 'return this.date.getUTCMonth() == 2 && this.date.getUTCDay() == 3;'}"));
	}

	@Test
	public void bindValueToDbRef() {

		Document target = parse("{ 'reference' : { $ref : 'reference', $id : ?0 }}", "kohlin");
		assertThat(target).isEqualTo(Document.parse("{ 'reference' : { $ref : 'reference', $id : 'kohlin' }}"));
	}

	@Test
	public void bindToKey() {

		Document target = parse("{ ?0 : ?1 }", "firstname", "kaladin");
		assertThat(target).isEqualTo(Document.parse("{ 'firstname' : 'kaladin' }"));
	}

	@Test
	public void bindListValue() {

		//
		Document target = parse("{ 'lastname' : { $in : ?0 } }", Arrays.asList("Kohlin", "Davar"));
		assertThat(target).isEqualTo(Document.parse("{ 'lastname' : { $in : ['Kohlin', 'Davar' ]} }"));
	}

	@Test
	public void bindListOfBinaryValue() {

		//
		byte[] value = "Kohlin".getBytes(StandardCharsets.UTF_8);
		List<byte[]> args = Collections.singletonList(value);

		Document target = parse("{ 'lastname' : { $in : ?0 } }", args);
		assertThat(target).isEqualTo(new Document("lastname", new Document("$in", args)));
	}

	@Test
	public void bindExtendedExpression() {

		Document target = parse("{'id':?#{ [0] ? { $exists :true} : [1] }}", true, "firstname", "kaladin");
		assertThat(target).isEqualTo(Document.parse("{ \"id\" : { \"$exists\" : true}}"));
	}

	// {'id':?#{ [0] ? { $exists :true} : [1] }}

	@Test
	public void bindDocumentValue() {

		//
		Document target = parse("{ 'lastname' : ?0 }", new Document("$eq", "Kohlin"));
		assertThat(target).isEqualTo(Document.parse("{ 'lastname' : { '$eq' : 'Kohlin' } }"));
	}

	@Test
	public void arrayWithoutBinding() {

		//
		Document target = parse("{ 'lastname' : { $in : [\"Kohlin\", \"Davar\"] } }");
		assertThat(target).isEqualTo(Document.parse("{ 'lastname' : { $in : ['Kohlin', 'Davar' ]} }"));
	}

	@Test
	public void bindSpEL() {

		// "{ arg0 : ?#{[0]} }"
		Document target = parse("{ arg0 : ?#{[0]} }", 100.01D);
		assertThat(target).isEqualTo(new Document("arg0", 100.01D));
	}

	private static Document parse(String json, Object... args) {

		ParameterBindingJsonReader reader = new ParameterBindingJsonReader(json, args);
		return new ParameterBindingDocumentCodec().decode(reader, DecoderContext.builder().build());
	}

}
