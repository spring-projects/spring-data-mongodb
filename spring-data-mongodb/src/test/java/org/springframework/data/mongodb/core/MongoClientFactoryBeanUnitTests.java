/*
 * Copyright 2019. the original author or authors.
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

import java.util.concurrent.TimeUnit;

import com.mongodb.ServerAddress;
import org.junit.jupiter.api.Test;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;

/**
 * @author Christoph Strobl
 */
class MongoClientFactoryBeanUnitTests {

	static final String CONNECTION_STRING_STRING = "mongodb://db1.example.net:27017,db2.example.net:2500/?replicaSet=test&connectTimeoutMS=300000";
	static final ConnectionString CONNECTION_STRING = new ConnectionString(CONNECTION_STRING_STRING);

	@Test // DATAMONGO-2427
	void connectionStringParametersNotOverriddenByDefaults() {

		MongoClientFactoryBean factoryBean = new MongoClientFactoryBean();
		factoryBean.setConnectionString(CONNECTION_STRING);
		factoryBean.setMongoClientSettings(MongoClientSettings.builder().build());

		MongoClientSettings settings = factoryBean.computeClientSetting();

		assertThat(settings.getClusterSettings().getRequiredReplicaSetName()).isEqualTo("test");
		assertThat(settings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS)).isEqualTo(300000);
		assertThat(settings.getClusterSettings().getHosts()).hasSize(2);
	}

	@Test // DATAMONGO-2427
	void hostPortParametersNotOverriddenByDefaults() {

		MongoClientFactoryBean factoryBean = new MongoClientFactoryBean();
		factoryBean.setPort(2500);
		factoryBean.setHost("db2.example.net");
		factoryBean.setReplicaSet("rs0");
		factoryBean.setMongoClientSettings(MongoClientSettings.builder().build());

		MongoClientSettings settings = factoryBean.computeClientSetting();

		assertThat(settings.getClusterSettings().getRequiredReplicaSetName()).isEqualTo("rs0");
		assertThat(settings.getClusterSettings().getHosts()).containsExactly(new ServerAddress("db2.example.net", 2500));
	}

	@Test // DATAMONGO-2427
	void explicitSettingsOverrideConnectionStringOnes() {

		MongoClientFactoryBean factoryBean = new MongoClientFactoryBean();
		factoryBean.setConnectionString(CONNECTION_STRING);
		factoryBean.setMongoClientSettings(
				MongoClientSettings.builder().applyToClusterSettings(it -> it.requiredReplicaSetName("rs0"))
						.applyToSocketSettings(it -> it.connectTimeout(100, TimeUnit.MILLISECONDS)).build());

		MongoClientSettings settings = factoryBean.computeClientSetting();

		assertThat(settings.getClusterSettings().getRequiredReplicaSetName()).isEqualTo("rs0");
		assertThat(settings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS)).isEqualTo(100);
		assertThat(settings.getClusterSettings().getHosts()).hasSize(2);
	}

	@Test // DATAMONGO-2427
	void hostAndPortPlusConnectionStringError() throws Exception {

		MongoClientFactoryBean factoryBean = new MongoClientFactoryBean();
		factoryBean.setConnectionString(CONNECTION_STRING);
		factoryBean.setHost("localhost");
		factoryBean.setPort(27017);
		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> factoryBean.createInstance());
	}
}
