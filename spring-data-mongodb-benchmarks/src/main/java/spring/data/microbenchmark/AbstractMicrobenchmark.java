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
package spring.data.microbenchmark;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.springframework.core.env.StandardEnvironment;
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

	@Test
	public void run() throws Exception {
		new Runner(options().build()).run();
	}

	protected ChainedOptionsBuilder options() throws Exception {

		String className = org.springframework.util.ClassUtils.getShortName(getClass());

		ChainedOptionsBuilder optionsBuilder = new OptionsBuilder().include(".*" + className + ".*").jvmArgs(jvmArgs());
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

	private ChainedOptionsBuilder measure(ChainedOptionsBuilder optionsBuilder) {

		int measurementIterations = getMeasurementIterations();
		if (measurementIterations <= 0) {
			return optionsBuilder;
		}

		return optionsBuilder.measurementIterations(measurementIterations);
	}

	private ChainedOptionsBuilder warmup(ChainedOptionsBuilder optionsBuilder) {

		int warmupIterations = getWarmupIterations();
		if (warmupIterations <= 0) {
			return optionsBuilder;
		}

		return optionsBuilder.warmupIterations(warmupIterations);
	}

	private ChainedOptionsBuilder forks(ChainedOptionsBuilder optionsBuilder) {

		int forks = getForksCount();
		if (forks <= 0) {
			return optionsBuilder;
		}

		return optionsBuilder.forks(forks);
	}

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
	 * {@code project.version_yyyy-MM-dd_ClassName.json} eg. {@literal 1.11.0.BUILD-SNAPSHOT_2017-03-07_MappingMongoConverterBenchmark.json}
	 * @return
	 */
	private String reportFilename() {

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
}
