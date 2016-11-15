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

package org.apache.storm.starter;

import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import org.apache.storm.starter.bolt.ProcessingBolt;
import org.apache.storm.starter.bolt.PrinterBolt;
import org.apache.storm.starter.spout.Q2TwitterSampleSpout;
import org.apache.storm.starter.spout.Q2RandomIntegerSpout;
import org.apache.storm.starter.spout.Q2RandomHashtagSpout;

import org.apache.storm.starter.util.StormRunner;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;




public class CS838Assignment2PartC2 {        

	public static void main(String[] args) throws Exception {
		String consumerKey = args[0]; 
		String consumerSecret = args[1]; 
		String accessToken = args[2]; 
		String accessTokenSecret = args[3];
		String localOrCluster = args[4];
		String[] arguments = args.clone();
		String[] keyWords = Arrays.copyOfRange(arguments, 4, arguments.length);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("twitter", new Q2TwitterSampleSpout(consumerKey, consumerSecret,
					accessToken, accessTokenSecret, keyWords));
		builder.setSpout("numGen", new Q2RandomIntegerSpout());
		builder.setSpout("hasGen", new Q2RandomHashtagSpout());
		builder.setBolt("print", new ProcessingBolt()).shuffleGrouping("numGen").shuffleGrouping("hasGen").shuffleGrouping("twitter");

		Config conf = new Config();

		if(localOrCluster.compareTo("local")==0) 
		{ 
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Q2_local", conf, builder.createTopology());
			Utils.sleep(3000000);
			cluster.shutdown();
		}
		else if (localOrCluster.compareTo("remote")==0)
		{
			StormRunner.runTopologyRemotely(builder.createTopology(), "Q2_remote", conf) ;
		}

	}
}
