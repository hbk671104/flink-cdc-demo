/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.tbsoft.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.oceanbase.OceanBaseSource;
import com.ververica.cdc.connectors.oceanbase.table.StartupMode;

import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * https://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		SourceFunction<String> sourceFunction = OceanBaseSource.<String>builder()
            .rsList("127.0.0.1:2882:2881")  // set root server list
            .startupMode(StartupMode.INITIAL) // set startup mode
            .username("root@test")  // set cluster username
            .password("root")  // set cluster password
            .tenantName("test")  // set captured tenant name, do not support regex
            .databaseName("test")  // set captured database, support regex
            .tableName("test_table")  // set captured table, support regex
            .hostname("127.0.0.1")  // set hostname of OceanBase server or proxy
            .port(2881)  // set the sql port for OceanBase server or proxy
            .logProxyHost("127.0.0.1")  // set the hostname of log proxy
            .logProxyPort(2983)  // set the port of log proxy
            .deserializer(new JsonDebeziumDeserializationSchema())  // converts SourceRecord to JSON String
            .build();

		// enable checkpoint
		env.enableCheckpointing(3000);

		env
		.addSource(sourceFunction)
		.print()
		.setParallelism(1); // use parallelism 1 for sink to keep message ordering
		
		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
