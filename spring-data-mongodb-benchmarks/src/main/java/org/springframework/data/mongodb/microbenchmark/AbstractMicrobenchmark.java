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
package org.springframework.data.mongodb.microbenchmark;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

import org.junit.Test;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.data.mongodb.microbenchmark.ResultsWriter.Utils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 */
@Warmup(iterations = AbstractMicrobenchmark.WARMUP_ITERATIONS)
@Measurement(iterations = AbstractMicrobenchmark.MEASUREMENT_ITERATIONS)
@Fork(AbstractMicrobenchmark.FORKS)
@State(Scope.Thread)
public class AbstractMicrobenchmark {

	static final int WARMUP_ITERATIONS = 5;
	static final int MEASUREMENT_ITERATIONS = 10;
	static final int FORKS = 1;
	static final String[] JVM_ARGS = { "-server", "-XX:+HeapDumpOnOutOfMemoryError", "-Xms1024m", "-Xmx1024m",
			"-XX:MaxDirectMemorySize=1024m" };

	private final StandardEnvironment environment = new StandardEnvironment();

	/**
	 * Run matching {@link org.openjdk.jmh.annotations.Benchmark} methods with options collected from
	 * {@link org.springframework.core.env.Environment}.
	 *
	 * @throws Exception
	 * @see #options(String)
	 */
	@Test
	public void run() throws Exception {

		String includes = includes();

		if (!includes.contains(org.springframework.util.ClassUtils.getShortName(getClass()))) {
			return;
		}

		publishResults(new Runner(options(includes).build()).run());
	}

	/**
	 * Get the regex for all benchmarks to be included in the run. By default every benchmark within classes matching the
	 * current ones short name. <br />
	 * The {@literal benchmark} command line argument allows overriding the defaults using {@code #} as class / method
	 * name separator.
	 *
	 * @return never {@literal null}.
	 * @see org.springframework.util.ClassUtils#getShortName(Class)
	 */
	protected String includes() {

		String tests = environment.getProperty("benchmark", String.class);

		if (!StringUtils.hasText(tests)) {
			return ".*" + org.springframework.util.ClassUtils.getShortName(getClass()) + ".*";
		}

		if (!tests.contains("#")) {
			return ".*" + tests + ".*";
		}

		String[] args = tests.split("#");
		return ".*" + args[0] + "." + args[1];
	}

	/**
	 * Collect all options for the {@link Runner}.
	 *
	 * @param includes regex for matching benchmarks to be included in the run.
	 * @return never {@literal null}.
	 * @throws Exception
	 */
	protected ChainedOptionsBuilder options(String includes) throws Exception {

		ChainedOptionsBuilder optionsBuilder = new OptionsBuilder().include(includes).jvmArgs(jvmArgs());

		optionsBuilder = warmup(optionsBuilder);
		optionsBuilder = measure(optionsBuilder);
		optionsBuilder = forks(optionsBuilder);
		optionsBuilder = report(optionsBuilder);

		return optionsBuilder;
	}

	/**
	 * JVM args to apply to {@link Runner} via its {@link org.openjdk.jmh.runner.options.Options}.
	 *
	 * @return {@link #JVM_ARGS} by default.
	 */
	protected String[] jvmArgs() {

		String[] args = new String[JVM_ARGS.length];
		System.arraycopy(JVM_ARGS, 0, args, 0, JVM_ARGS.length);
		return args;
	}

	/**
	 * Read {@code warmupIterations} property from {@link org.springframework.core.env.Environment}.
	 *
	 * @return -1 if not set.
	 */
	protected int getWarmupIterations() {
		return environment.getProperty("warmupIterations", Integer.class, -1);
	}

	/**
	 * Read {@code measurementIterations} property from {@link org.springframework.core.env.Environment}.
	 *
	 * @return -1 if not set.
	 */
	protected int getMeasurementIterations() {
		return environment.getProperty("measurementIterations", Integer.class, -1);

	}

	/**
	 * Read {@code forks} property from {@link org.springframework.core.env.Environment}.
	 *
	 * @return -1 if not set.
	 */
	protected int getForksCount() {
		return environment.getProperty("forks", Integer.class, -1);
	}

	/**
	 * Read {@code benchmarkReportDir} property from {@link org.springframework.core.env.Environment}.
	 *
	 * @return {@literal null} if not set.
	 */
	protected String getReportDirectory() {
		return environment.getProperty("benchmarkReportDir");
	}

	/**
	 * Read {@code measurementTime} property from {@link org.springframework.core.env.Environment}.
	 *
	 * @return -1 if not set.
	 */
	protected long getMeasurementTime() {
		return environment.getProperty("measurementTime", Long.class, -1L);
	}

	/**
	 * Read {@code warmupTime} property from {@link org.springframework.core.env.Environment}.
	 *
	 * @return -1 if not set.
	 */
	protected long getWarmupTime() {
		return environment.getProperty("warmupTime", Long.class, -1L);
	}

	/**
	 * {@code project.version_yyyy-MM-dd_ClassName.json} eg.
	 * {@literal 1.11.0.BUILD-SNAPSHOT_2017-03-07_MappingMongoConverterBenchmark.json}
	 *
	 * @return
	 */
	protected String reportFilename() {

		StringBuilder sb = new StringBuilder();

		if (environment.containsProperty("project.version")) {

			sb.append(environment.getProperty("project.version"));
			sb.append("_");
		}

		sb.append(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
		sb.append("_");
		sb.append(org.springframework.util.ClassUtils.getShortName(getClass()));
		sb.append(".json");
		return sb.toString();
	}

	/**
	 * Apply measurement options to {@link ChainedOptionsBuilder}.
	 *
	 * @param optionsBuilder must not be {@literal null}.
	 * @return {@link ChainedOptionsBuilder} with options applied.
	 * @see #getMeasurementIterations()
	 * @see #getMeasurementTime()
	 */
	private ChainedOptionsBuilder measure(ChainedOptionsBuilder optionsBuilder) {

		int measurementIterations = getMeasurementIterations();
		long measurementTime = getMeasurementTime();

		if (measurementIterations > 0) {
			optionsBuilder = optionsBuilder.measurementIterations(measurementIterations);
		}

		if (measurementTime > 0) {
			optionsBuilder = optionsBuilder.measurementTime(TimeValue.seconds(measurementTime));
		}

		return optionsBuilder;
	}

	/**
	 * Apply warmup options to {@link ChainedOptionsBuilder}.
	 *
	 * @param optionsBuilder must not be {@literal null}.
	 * @return {@link ChainedOptionsBuilder} with options applied.
	 * @see #getWarmupIterations()
	 * @see #getWarmupTime()
	 */
	private ChainedOptionsBuilder warmup(ChainedOptionsBuilder optionsBuilder) {

		int warmupIterations = getWarmupIterations();
		long warmupTime = getWarmupTime();

		if (warmupIterations > 0) {
			optionsBuilder = optionsBuilder.warmupIterations(warmupIterations);
		}

		if (warmupTime > 0) {
			optionsBuilder = optionsBuilder.warmupTime(TimeValue.seconds(warmupTime));
		}

		return optionsBuilder;
	}

	/**
	 * Apply forks option to {@link ChainedOptionsBuilder}.
	 *
	 * @param optionsBuilder must not be {@literal null}.
	 * @return {@link ChainedOptionsBuilder} with options applied.
	 * @see #getForksCount()
	 */
	private ChainedOptionsBuilder forks(ChainedOptionsBuilder optionsBuilder) {

		int forks = getForksCount();

		if (forks <= 0) {
			return optionsBuilder;
		}

		return optionsBuilder.forks(forks);
	}

	/**
	 * Apply report option to {@link ChainedOptionsBuilder}.
	 *
	 * @param optionsBuilder must not be {@literal null}.
	 * @return {@link ChainedOptionsBuilder} with options applied.
	 * @throws IOException if report file cannot be created.
	 * @see #getReportDirectory()
	 */
	private ChainedOptionsBuilder report(ChainedOptionsBuilder optionsBuilder) throws IOException {

		String reportDir = getReportDirectory();

		if (!StringUtils.hasText(reportDir)) {
			return optionsBuilder;
		}

		String reportFilePath = reportDir + (reportDir.endsWith(File.separator) ? "" : File.separator) + reportFilename();
		File file = ResourceUtils.getFile(reportFilePath);

		if (file.exists()) {
			file.delete();
		} else {

			file.getParentFile().mkdirs();
			file.createNewFile();
		}

		optionsBuilder.resultFormat(ResultFormatType.JSON);
		optionsBuilder.result(reportFilePath);

		return optionsBuilder;
	}

	/**
	 * Publish results to an external system.
	 *
	 * @param results must not be {@literal null}.
	 */
	private void publishResults(Collection<RunResult> results) {

		if (CollectionUtils.isEmpty(results) || !environment.containsProperty("publishTo")) {
			return;
		}

		String uri = environment.getProperty("publishTo");
		try {
			Utils.forUri(uri).write(results);
		} catch (Exception e) {
			System.err.println(String.format("Cannot save benchmark results to '%s'. Error was %s.", uri, e));
		}
	}
}
