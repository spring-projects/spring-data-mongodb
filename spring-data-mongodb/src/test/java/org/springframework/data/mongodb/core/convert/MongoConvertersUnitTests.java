/*
 * Copyright 2011-2019 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import static org.assertj.core.api.Assertions.*;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Currency;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.assertj.core.data.TemporalUnitLessThanOffset;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.Test;
import org.springframework.core.convert.support.ConfigurableConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.geo.Shape;
import org.springframework.data.mongodb.core.convert.MongoConverters.AtomicIntegerToIntegerConverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.AtomicLongToLongConverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.BigDecimalToStringConverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.BsonTimestampToInstantConverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.CurrencyToStringConverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.IntegerToAtomicIntegerConverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.LongToAtomicLongConverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.StringToBigDecimalConverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.StringToCurrencyConverter;
import org.springframework.data.mongodb.core.geo.Sphere;

/**
 * Unit tests for {@link MongoConverters}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
public class MongoConvertersUnitTests {

	@Test
	public void convertsBigDecimalToStringAndBackCorrectly() {

		BigDecimal bigDecimal = BigDecimal.valueOf(254, 1);
		String value = BigDecimalToStringConverter.INSTANCE.convert(bigDecimal);
		assertThat(value).isEqualTo("25.4");

		BigDecimal reference = StringToBigDecimalConverter.INSTANCE.convert(value);
		assertThat(reference).isEqualTo(bigDecimal);
	}

	@Test // DATAMONGO-858
	public void convertsBoxToDocumentAndBackCorrectly() {

		Box box = new Box(new Point(1, 2), new Point(3, 4));

		Document document = GeoConverters.BoxToDocumentConverter.INSTANCE.convert(box);
		Shape shape = GeoConverters.DocumentToBoxConverter.INSTANCE.convert(document);

		assertThat(shape).isEqualTo(box);
	}

	@Test // DATAMONGO-858
	public void convertsCircleToDocumentAndBackCorrectly() {

		Circle circle = new Circle(new Point(1, 2), 3);

		Document document = GeoConverters.CircleToDocumentConverter.INSTANCE.convert(circle);
		Shape shape = GeoConverters.DocumentToCircleConverter.INSTANCE.convert(document);

		assertThat(shape).isEqualTo(circle);
	}

	@Test // DATAMONGO-858
	public void convertsPolygonToDocumentAndBackCorrectly() {

		Polygon polygon = new Polygon(new Point(1, 2), new Point(2, 3), new Point(3, 4), new Point(5, 6));

		Document document = GeoConverters.PolygonToDocumentConverter.INSTANCE.convert(polygon);
		Shape shape = GeoConverters.DocumentToPolygonConverter.INSTANCE.convert(document);

		assertThat(shape).isEqualTo(polygon);
	}

	@Test // DATAMONGO-858
	public void convertsSphereToDocumentAndBackCorrectly() {

		Sphere sphere = new Sphere(new Point(1, 2), 3);

		Document document = GeoConverters.SphereToDocumentConverter.INSTANCE.convert(sphere);
		org.springframework.data.geo.Shape shape = GeoConverters.DocumentToSphereConverter.INSTANCE.convert(document);

		assertThat(shape).isEqualTo(sphere);
	}

	@Test // DATAMONGO-858
	public void convertsPointToListAndBackCorrectly() {

		Point point = new Point(1, 2);

		Document document = GeoConverters.PointToDocumentConverter.INSTANCE.convert(point);
		org.springframework.data.geo.Point converted = GeoConverters.DocumentToPointConverter.INSTANCE.convert(document);

		assertThat(converted).isEqualTo(point);
	}

	@Test // DATAMONGO-1372
	public void convertsCurrencyToStringCorrectly() {
		assertThat(CurrencyToStringConverter.INSTANCE.convert(Currency.getInstance("USD"))).isEqualTo("USD");
	}

	@Test // DATAMONGO-1372
	public void convertsStringToCurrencyCorrectly() {
		assertThat(StringToCurrencyConverter.INSTANCE.convert("USD")).isEqualTo(Currency.getInstance("USD"));
	}

	@Test // DATAMONGO-1416
	public void convertsAtomicLongToLongCorrectly() {
		assertThat(AtomicLongToLongConverter.INSTANCE.convert(new AtomicLong(100L))).isEqualTo(100L);
	}

	@Test // DATAMONGO-1416
	public void convertsAtomicIntegerToIntegerCorrectly() {
		assertThat(AtomicIntegerToIntegerConverter.INSTANCE.convert(new AtomicInteger(100))).isEqualTo(100);
	}

	@Test // DATAMONGO-1416
	public void convertsLongToAtomicLongCorrectly() {
		assertThat(LongToAtomicLongConverter.INSTANCE.convert(100L)).isInstanceOf(AtomicLong.class);
	}

	@Test // DATAMONGO-1416
	public void convertsIntegerToAtomicIntegerCorrectly() {
		assertThat(IntegerToAtomicIntegerConverter.INSTANCE.convert(100)).isInstanceOf(AtomicInteger.class);
	}

	@Test // DATAMONGO-2113
	public void convertsBsonTimestampToInstantCorrectly() {

		assertThat(BsonTimestampToInstantConverter.INSTANCE.convert(new BsonTimestamp(6615900307735969796L)))
				.isCloseTo(Instant.ofEpochSecond(1540384327), new TemporalUnitLessThanOffset(100, ChronoUnit.MILLIS));
	}

	@Test // DATAMONGO-2210
	public void convertsUrisToString() {

		MongoCustomConversions conversions = new MongoCustomConversions();

		assertThat(conversions.getSimpleTypeHolder().isSimpleType(URI.class)).isTrue();

		ConfigurableConversionService conversionService = new DefaultConversionService();
		conversions.registerConvertersIn(conversionService);

		assertThat(conversionService.convert(URI.create("/segment"), String.class)).isEqualTo("/segment");
		assertThat(conversionService.convert("/segment", URI.class)).isEqualTo(URI.create("/segment"));
	}
}
