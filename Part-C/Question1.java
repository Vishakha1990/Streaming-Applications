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

import org.apache.storm.starter.bolt.PrinterBolt;
import org.apache.storm.starter.bolt.ClusterPrinterBolt;
import org.apache.storm.starter.spout.TwitterSampleSpout;
import org.apache.storm.starter.util.StormRunner;


public class Question1 {        
  
     public static void main(String[] args) throws Exception {
        String consumerKey = args[0]; 
        String consumerSecret = args[1]; 
        String accessToken = args[2]; 
        String accessTokenSecret = args[3];
	String localOrCluster = args[4];
        String[] arguments = args.clone();
        //String[] keyWords = Arrays.copyOfRange(arguments, 4, arguments.length);
        String[] keyWords = new String[]{"Clinton","Trump", "debate", "win", "TheWalkingDead", "android", "WonderWoman", "Series", "Election", "USA", "GOT7", "OneDirection", "Empire", "Apple"};
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter", new TwitterSampleSpout(consumerKey, consumerSecret,
                                accessToken, accessTokenSecret, keyWords));



        if(localOrCluster.compareTo("local")==0) 
  	{
	builder.setBolt("print", new PrinterBolt())
                .shuffleGrouping("twitter");
         }               
	else if (localOrCluster.compareTo("remote")==0)
	{
	       builder.setBolt("print", new ClusterPrinterBolt())
                .shuffleGrouping("twitter");

}
        Config conf = new Config();
  
        if(localOrCluster.compareTo("local")==0) 
	{
        	LocalCluster cluster = new LocalCluster();
        	cluster.submitTopology("test", conf, builder.createTopology());
		//Utils.sleep(10000);
		//cluster.shutdown();
	}
	else if (localOrCluster.compareTo("remote")==0)
	{
		StormRunner.runTopologyRemotely(builder.createTopology(), "test_remote", conf) ;
	}
    }
}

