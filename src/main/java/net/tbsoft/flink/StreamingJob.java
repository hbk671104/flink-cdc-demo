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

import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import com.oceanbase.clogproxy.client.LogProxyClient;
import com.oceanbase.clogproxy.client.exception.LogProxyClientException;
import com.oceanbase.clogproxy.client.listener.RecordListener;
import com.oceanbase.oms.logmessage.LogMessage;

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
		ObReaderConfig config = new ObReaderConfig();
		// 设置OceanBase root server 地址列表，格式为（可以支持多个，用';'分隔）：ip1:rpc_port1:sql_port1;ip2:rpc_port2:sql_port2
		config.setRsList("127.0.0.1:2882:2881");
		// 设置用户名和密码（非系统租户）
		config.setUsername("root@sys");
		config.setPassword("root");
		// 设置启动位点（UNIX时间戳，单位s）, 0表示从当前时间启动。
		config.setStartTimestamp(0L);
		// 设置订阅表白名单，格式为：tenant.db.table, '*'表示通配.
		config.setTableWhiteList("test.test.test_table");

		// 指定oblogproxy服务地址，创建实例.
		LogProxyClient client = new LogProxyClient("127.0.0.1", 2983, config);
		// 添加 RecordListener
		client.addListener(new RecordListener() {
			@Override
			public void notify(LogMessage message){
				// 处理消息
				System.out.println(message);
			}

			@Override
			public void onException(LogProxyClientException e) {
				// 处理错误
				if (e.needStop()) {
					// 不可恢复异常，需要停止Client
					client.stop();
				}
			}
		});

		// 启动
		client.start();
		client.join();
	}
}
