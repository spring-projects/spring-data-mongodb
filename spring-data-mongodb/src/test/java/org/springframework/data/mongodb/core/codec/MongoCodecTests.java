/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.mongodb.core.codec;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import lombok.Data;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.client.model.Filters;

/**
 * @author Christoph Strobl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class MongoCodecTests {

	@Autowired MongoTemplate template;

	@Configuration
	static class Config extends AbstractMongoConfiguration {

		@Override
		public MongoClient mongoClient() {

			CodecRegistry reg = CodecRegistries.fromRegistries(MongoClient.getDefaultCodecRegistry(),
					CodecRegistries.fromCodecs(new RootTypeCodec()));

			MongoClient client = new MongoClient("localhost:27017", MongoClientOptions.builder().codecRegistry(reg).build());
			return client;
		}

		@Override
		protected String getDatabaseName() {
			return "spring-codec";
		}
	}

	@Test // DATAMONGO-1175
	public void shouldUseConverterCodecRegistryOnRootType() {

		TypeHavingCodecRegistered source = new TypeHavingCodecRegistered();
		template.save(source);

		Document document = template.getCollection(template.getCollectionName(TypeHavingCodecRegistered.class))
				.find(Filters.eq("_id", new ObjectId(source.id))).first();

		assertThat(document).containsEntry("customName", "o.O").doesNotContainKey("_class");

		TypeHavingCodecRegistered loaded = template.findOne(query(where("id").is(source.id)),
				TypeHavingCodecRegistered.class);
		assertThat(loaded.name).isEqualTo("o.O");
	}

	@Test // DATAMONGO-1175
	public void writesAndReadsTypeWithCodecWhenUsedAsNestedObject() {

		Wrapper source = new Wrapper();
		source.withCodec = new TypeHavingCodecRegistered();

		template.save(source);

		Document document = template.getCollection(template.getCollectionName(Wrapper.class))
				.find(Filters.eq("_id", new ObjectId(source.id))).first();

		assertThat(document.get("withCodec", Document.class)).containsEntry("customName", "o.O");

		Wrapper loaded = template.findOne(query(where("id").is(source.id)), Wrapper.class);
		assertThat(loaded.withCodec.name).isEqualTo("o.O");
	}

	@Test // DATAMONGO-1175
	public void shouldNotUseConverterCodecRegistry() {

		NonCodecType source = new NonCodecType();
		template.save(source);

		Document document = template.getCollection(template.getCollectionName(NonCodecType.class))
				.find(Filters.eq("_id", new ObjectId(source.id))).first();

		assertThat(document).containsKey("_class").doesNotContainKey("customName");

		NonCodecType loaded = template.findOne(query(where("id").is(source.id)), NonCodecType.class);
		assertThat(loaded).isNotNull();
	}

	@Data
	static class NonCodecType {

		@Id String id;
		String name;
	}

	@Data
	static class Wrapper {

		@Id String id;
		TypeHavingCodecRegistered withCodec;
	}

	@Data
	static class TypeHavingCodecRegistered {

		@Id String id;
		String name;
	}

	static class RootTypeCodec implements Codec<TypeHavingCodecRegistered> {

		@Override
		public void encode(BsonWriter writer, TypeHavingCodecRegistered value, EncoderContext encoderContext) {

			writer.writeStartDocument();

			writer.writeObjectId("_id", new ObjectId());
			writer.writeString("customName", (value.name != null ? (value.name + " ") : "") + "o.O");
			writer.writeEndDocument();
		}

		@Override
		public Class<TypeHavingCodecRegistered> getEncoderClass() {
			return TypeHavingCodecRegistered.class;
		}

		@Override
		public TypeHavingCodecRegistered decode(BsonReader reader, DecoderContext decoderContext) {

			TypeHavingCodecRegistered foo = new TypeHavingCodecRegistered();
			reader.readStartDocument();
			foo.id = reader.readObjectId("_id").toHexString();
			foo.name = reader.readString("customName");
			reader.readEndDocument();
			return foo;
		}
	}
}
