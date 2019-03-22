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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;

import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.lang.reflect.Field;
import java.util.stream.Stream;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

/**
 * @author Christoph Strobl
 */
public class Fluxperiment {

	@Test
	public void applySkipFromFlux() {

		hackedFlux().skip(3) //
				.as(StepVerifier::create) //
				.expectAccessibleContext().assertThat(ctx -> {

					assertThat(ctx.getOrEmpty("skip")).contains(3L);
					assertThat(ctx.getOrEmpty("take")).isEmpty();
				}).then() //
				.expectNext("4") //
				.expectNext("5") //
				.verifyComplete();
	}

	@Test
	public void applyTakeFromFlux() {

		hackedFlux().limitRequest(3) //
				.as(StepVerifier::create) //
				.expectAccessibleContext().assertThat(ctx -> {

					assertThat(ctx.getOrEmpty("skip")).isEmpty();
					assertThat(ctx.getOrEmpty("take")).contains(3L);
				}).then() //
				.expectNext("1") //
				.expectNext("2") //
				.expectNext("3") //
				.verifyComplete();
	}

	@Test
	public void applySkipAndLimitFromFlux/* in that order */() {

		hackedFlux().skip(1) /* in DB */.limitRequest(2) /* in DB */ //
				.as(StepVerifier::create) //
				.expectAccessibleContext().assertThat(ctx -> {

					assertThat(ctx.getOrEmpty("skip")).contains(1L);
					assertThat(ctx.getOrEmpty("take")).contains(2L);
				}).then() //
				.expectNext("2") //
				.expectNext("3") //
				.verifyComplete();
	}

	@Test
	public void applyTakeButNotSkipFromFlux/* cause order matters */() {

		hackedFlux().limitRequest(3)/* in DB */.skip(1) /* in memory */ //
				.as(StepVerifier::create) //
				.expectAccessibleContext().assertThat(ctx -> {

					assertThat(ctx.getOrEmpty("skip")).isEmpty();
					assertThat(ctx.getOrEmpty("take")).contains(3L);
				}).then() //
				.expectNext("2") //
				.expectNext("3") //
				.verifyComplete();
	}

	@Test
	public void justApplySkipButNotTakeIfTheyDoNotFollowOneAnother() {

		hackedFlux().skip(1)/* in DB */.map(v -> v).limitRequest(2) /* in memory */ //
				.as(StepVerifier::create) //
				.expectAccessibleContext().assertThat(ctx -> {

					assertThat(ctx.getOrEmpty("skip")).contains(1L);
					assertThat(ctx.getOrEmpty("take")).isEmpty();
				}).then() //
				.expectNext("2") //
				.expectNext("3") //
				.verifyComplete();
	}

	@Test
	public void applyNeitherSkipNorTakeIfPrecededWithOtherOperator() {

		hackedFlux().map(v -> v).skip(1).limitRequest(2) //
				.as(StepVerifier::create) //
				.expectAccessibleContext().assertThat(ctx -> {

					assertThat(ctx.getOrEmpty("skip")).isEmpty();
					assertThat(ctx.getOrEmpty("take")).isEmpty();
				}).then() //
				.expectNext("2") //
				.expectNext("3") //
				.verifyComplete();
	}

	@Test
	public void applyOnlyFirstSkip/* toDatabase */() {

		hackedFlux().skip(3)/* in DB */.skip(1)/* in memory */ //
				.as(StepVerifier::create) //
				.expectAccessibleContext().assertThat(ctx -> {

					assertThat(ctx.getOrEmpty("skip")).contains(3L);
					assertThat(ctx.getOrEmpty("take")).isEmpty();
				}).then() //
				.expectNext("5") //
				.verifyComplete();
	}

	Flux<String> hackedFlux() {

		return new Flux<String>() {

			@Override
			public void subscribe(CoreSubscriber actual) {

				Long skip = extractSkip(actual);
				Long take = extractLimit(actual);

				System.out.println(String.format("Using offset: %s and limit: %s", skip, take));

				// and here we use the original Flux and evaluate skip / take in the template
				Stream<String> source = Stream.of("1", "2", "3", "4", "5");
				Context context = Context.empty();

				// and here we use the original Flux and evaluate skip / take in the template
				if (skip != null && skip > 0L) {
					context = context.put("skip", skip);
					source = source.skip(skip);
				}
				if (take != null && take > 0L) {

					context = context.put("take", take);
					source = source.limit(take);
				}

				Flux.fromStream(source).subscriberContext(context).subscribe(actual);

			}
		};
	}

	@Nullable
	static Long extractSkip(Subscriber subscriber) {

		if (subscriber == null || !ClassUtils.getShortName(subscriber.getClass()).endsWith("SkipSubscriber")) {
			return null;
		}

		java.lang.reflect.Field field = ReflectionUtils.findField(subscriber.getClass(), "remaining");
		if (field == null) {
			return null;
		}

		ReflectionUtils.makeAccessible(field);
		Long skip = (Long) ReflectionUtils.getField(field, subscriber);
		if (skip != null && skip > 0L) {

			// reset the field, otherwise we'd skip stuff in the code.
			ReflectionUtils.setField(field, subscriber, 0L);
		}

		return skip;
	}

	@Nullable
	static Long extractLimit(Subscriber subscriber) {

		if (subscriber == null) {
			return null;
		}

		if (!ClassUtils.getShortName(subscriber.getClass()).endsWith("TakeSubscriber")
				&& !ClassUtils.getShortName(subscriber.getClass()).endsWith("FluxLimitRequestSubscriber")) {
			return extractLimit(extractPotentialTakeSubscriber(subscriber));
		}

		java.lang.reflect.Field field = ReflectionUtils.findField(subscriber.getClass(), "n"); // from TakeSubscriber
		if (field == null) {

			field = ReflectionUtils.findField(subscriber.getClass(), "toProduce"); // from FluxLimitRequestSubscriber
			if (field == null) {
				return null;
			}
		}

		ReflectionUtils.makeAccessible(field);
		return (Long) ReflectionUtils.getField(field, subscriber);
	}

	@Nullable
	static Subscriber extractPotentialTakeSubscriber(Subscriber subscriber) {

		if (!ClassUtils.getShortName(subscriber.getClass()).endsWith("SkipSubscriber")) {
			return null;
		}

		Field field = ReflectionUtils.findField(subscriber.getClass(), "actual");
		if (field == null) {
			return null;
		}

		ReflectionUtils.makeAccessible(field);
		return (Subscriber) ReflectionUtils.getField(field, subscriber);
	}
}
