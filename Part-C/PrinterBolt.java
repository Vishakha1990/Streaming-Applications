/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.starter.bolt;

import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.generated.Nimbus.Client;
import org.apache.storm.generated.Nimbus;
import java.io.*;
import java.util.*;

public class PrinterBolt extends BaseBasicBolt {

	public static int count = 0;

	@Override
		public void execute (Tuple tuple, BasicOutputCollector collector) {
			try {
				FileWriter fw = new FileWriter("Question1-LocalMode.txt", true);
				fw.write(tuple + "\n");
				fw.close();
			} catch (IOException e) {
				System.err.println(e);
			}
			count++;
			if(count == 500000) {
				try{
					Map conf = Utils.readStormConfig();
					conf.put("nimbus.seeds", "localhost");

					NimbusClient cc = NimbusClient.getConfiguredClient(conf);

					Nimbus.Client client = cc.getClient();
					client.killTopology("test");
				}catch (Exception e) {
					System.err.println(e);

				}


			}
		}



	@Override
		public void declareOutputFields(OutputFieldsDeclarer ofd) {
		}

}



