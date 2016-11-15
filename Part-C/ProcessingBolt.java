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
 * distributed under the License is distributed on an "AS IS" BASIS,  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.starter.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import java.util.*;
import java.io.*;
import twitter4j.*;


class ValueComparator implements Comparator {
	Map map;

	public ValueComparator(Map map) {
		this.map = map;
	}

	public int compare(Object keyA, Object keyB) {
		Comparable valueA = (Comparable) map.get(keyA);
		Comparable valueB = (Comparable) map.get(keyB);
		if(valueB.compareTo(valueA)==1)
			return 1;
		else if (valueB.compareTo(valueA)==0)
			return 1;
		else if (valueB.compareTo(valueA)==-1)
			return -1;
		return -1;
	}
}

public class ProcessingBolt extends BaseBasicBolt {
	private static int numFlag = 0;
	private static int hashFlag = 0;
	private static int tweetFlag = 0 ;
	Object numFriends;
	Object hashTag0;
	Object hashTag1;
	Object hashTag2;
	Object hashTag3;
	Object hashTag4;
	Object hashTag5;
	Object hashTag6;
	Object hashTag7;
	Object hashTag8;
	Object hashTag9;
	Vector<String> collectedtweets = new Vector<String>();
	Map<String, Integer> wordCountsMap = new HashMap<String, Integer>();
	int fileno = 0 ;
	int fileno1 = 0 ;
	List<String> FreeWordList = Arrays.asList ("a","about","above","after","again","against","all","am","an","and","any","are","aren't","as","at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours",	"ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves");


	public static Map sortByValue(Map unsortedMap) {
		Map sortedMap = new TreeMap(new ValueComparator(unsortedMap));
		sortedMap.putAll(unsortedMap);
		return sortedMap;
	}

	@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			if(tuple.getSourceComponent().compareTo("numGen")==0) {
				numFlag =1;
				tweetFlag = 0;
				numFriends = tuple.getValue(0) ;
			} else if(tuple.getSourceComponent().compareTo("hasGen")==0) {
				hashTag0 = tuple.getValue(0) ;
				hashTag1 = tuple.getValue(1) ;
				hashTag2 = tuple.getValue(2) ;
				hashTag3 = tuple.getValue(3) ;
				hashTag4 = tuple.getValue(4) ;
				hashTag5 = tuple.getValue(5) ;
				hashTag6 = tuple.getValue(6) ;
				hashTag7 = tuple.getValue(7) ;
				hashTag8 = tuple.getValue(8) ;
				hashTag9 = tuple.getValue(9) ;
				hashFlag =1 ;
				tweetFlag = 0;
			} else if((((tweetFlag == 0) &&((numFlag == 1) && (hashFlag == 1))) || ((tweetFlag == 1) &&((numFlag == 0) && (hashFlag == 0)))) && (tuple.getSourceComponent().compareTo("twitter")==0 )) {
				
				if((tweetFlag == 0) &&((numFlag == 1) && (hashFlag == 1))) {
					System.out.println("tweetsize" + collectedtweets.size());
					if(collectedtweets.size() > 0) 
					{
						try{
							FileWriter fw = new FileWriter("CollectedTweets" + fileno + ".txt", true);
							fw.write(collectedtweets + "\n");
							fileno++ ;
							fw.close();
						}catch (IOException e) {
							System.err.println(e);
						}

						collectedtweets.clear();
					}
					if(wordCountsMap.size() > 0) 
					{
						Map sortedMap = sortByValue(wordCountsMap);
						try{
							Set keyset = sortedMap.keySet(); 
							Object[] array = keyset.toArray() ;
							FileWriter fw = new FileWriter("TopWords" + fileno1 + ".txt", true);
							for(int i=0; i < array.length/2 ; i++ ) {
								fw.write(array[i] + "\n");
								fileno1++ ;
							}
							fw.close();

						}catch (IOException e) {
							System.err.println(e);
						}

						wordCountsMap.clear();			
					}
				}
				tweetFlag = 1 ;    
				numFlag =0 ;
				hashFlag =0 ;

				Status twitterStream = (Status) tuple.getValue(0) ;
				int friendscount = twitterStream.getUser().getFriendsCount() ;

				String tweet = twitterStream.getText() ;
				
				if (friendscount < (int)numFriends) {

if ((tweet.contains((String)hashTag9)) ||(tweet.contains((String)hashTag8)) ||(tweet.contains((String)hashTag7)) ||(tweet.contains((String)hashTag6)) ||(tweet.contains((String)hashTag5)) ||(tweet.contains((String)hashTag4)) || (tweet.contains((String)hashTag0)) || (tweet.contains((String)hashTag1)) || (tweet.contains((String)hashTag2)) || (tweet.contains((String)hashTag3))) {
		
			collectedtweets.add(tweet);
					for(String word:tweet.split("\\s",0)) {
						if(!FreeWordList.contains(word)) {
							Integer count = wordCountsMap.get(word);
							if (count == null)
								count = 0;
							count++;
							wordCountsMap.put(word, count);
						}
					}
				}
		}
			}
		}

	@Override
		public void declareOutputFields(OutputFieldsDeclarer ofd) {
		}

}




