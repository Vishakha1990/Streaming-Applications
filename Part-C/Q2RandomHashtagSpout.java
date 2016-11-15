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
package org.apache.storm.starter.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.*;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Emits a random integer and a timestamp value (offset by one day),
 * every 100 ms. The ts field can be used in tuple time based windowing.
 */
public class Q2RandomHashtagSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(Q2RandomIntegerSpout.class);
    private SpoutOutputCollector collector;
    private Random rand;
    private long msgId = 0;
    private String[] keyWords = new String[]{"job", "vote","local", "Clinton","Trump", "debate", "tech", "TheWalkingDead", "android", "Election", "USA", "GOT7", "Empire", "Apple", "ISIS", "election", "Football", "iPhone", "Amazon", "uber"};
    private String[] toSend = new String[10];
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag0", "hashtag1", "hasgtag2", "hashtag3", "hashtag4", "hashtag5", "hasgtag6", "hashtag7", "hashtag8", "hashtag9"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.rand = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(30000);

//    	Set<Integer> set = new HashSet<Integer>(SET_SIZE_REQUIRED);
//
//        while(set.size()< SET_SIZE_REQUIRED) {
//            if (set.add(rand.nextInt(NUMBER_RANGE)) != true)
//               continue;
//        }
//
//	Iterator<Integer> iter = set.iterator();
//	int i = 0;
//	while (iter.hasNext()) {
//	    toSend[i] = keyWords[iter.next().intValue()];
//	    i++;
//	}
	for (int i=0; i < 10; i++) {
	   toSend[i] = keyWords[rand.nextInt(19)];
	}

        collector.emit(new Values(toSend));
	System.out.println(toSend);
    }

    @Override
    public void ack(Object msgId) {
        LOG.debug("Got ACK for msgId : " + msgId);
    }

    @Override
    public void fail(Object msgId) {
        LOG.debug("Got FAIL for msgId : " + msgId);
    }
}
