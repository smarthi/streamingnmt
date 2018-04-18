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

package de.dws.berlin;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import akka.remote.serialization.ProtobufSerializer;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import de.dws.berlin.functions.SentenceDetectorFunction;
import de.dws.berlin.functions.SockeyeTranslateFunction;
import de.dws.berlin.serializer.SpanSerializer;
import de.dws.berlin.twitter.TweetJsonConverter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your appliation into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingNmt {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingNmt.class);

  private static SentenceModel engSentenceModel, deSentenceModel;
  private static TokenizerModel engTokenizerModel, deTokenizerModel;

  private static void initializeModels() throws IOException {
    engSentenceModel = new SentenceModel(StreamingNmt.class.getResource("/opennlp-models/en-sent.bin"));
    deSentenceModel = new SentenceModel(StreamingNmt.class.getResource("/opennlp-models/de-sent.bin"));
//    engTokenizerModel = new TokenizerModel(StreamingNmt.class.getResource("/opennlp-models/en-token.bin"));
    deTokenizerModel = new TokenizerModel((StreamingNmt.class.getResource("/opennlp-models/de-token.bin")));
  }

  public static void main(String[] args) throws Exception {

    initializeModels();

    ParameterTool parameterTool = ParameterTool.fromArgs(args);

    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment()
            .setParallelism(parameterTool.getInt("parallelism", 1))
            .setMaxParallelism(10);

    env.getConfig().enableObjectReuse();
    env.getConfig().registerTypeWithKryoSerializer(Span.class, SpanSerializer.class);
//    env.getConfig().registerTypeWithKryoSerializer(MyCustomType.class, ProtobufSerializer.class);
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

    // twitter credentials and source
    Properties props = new Properties();
    props.load(StreamingNmt.class.getResourceAsStream("/twitter.properties"));
    TwitterSource twitterSource = new TwitterSource(props);
//    twitterSource.setCustomEndpointInitializer(
//        new StreamingNmt.FilterEndpoint("#ShakespeareSunday, #SundayMotivation"));

    Set<String> langList = Stream.of(props.getProperty("twitter-source.langs"))
        .collect(Collectors.toSet());

    // Create a DataStream from TwitterSource filtered by deleted tweets
    // filter for en tweets
    DataStream<Tweet> twitterStream = env.addSource(twitterSource)
        .filter((FilterFunction<String>) value -> value.contains("created_at")) // filter out deleted tweets
        .flatMap(new TweetJsonConverter()) // convert JSON to Pojo
        .filter((FilterFunction<Tweet>) tweet -> langList.contains(tweet.getLanguage()) &&
        tweet.getText().length() > 0);
           // filter for tweets containing a URL
//        .filter((FilterFunction<Tweet>) value -> langList.contains(value.getLanguage())
//            && TweetURLMatcher.checkUrlInTweet(value))
//        // extract URL from tweet text
//        .flatMap(new ExtractUrlFromTweetFunction())
//        // extract html body content from URL
//        .flatMap(new ExtractTextFromHTML())
        // filter for URL redirects to tweets or zero content
//        .((FilterFunction<Tweet>) value -> value.getText().length() > 0);

   /// Language Detect and Split
//    SplitStream<String> splitStream = twitterStream.map(new LanguageDetectorFunction())
//        .split(new LanguageSelector());

    DataStream<Tuple2<String, String>> sentenceStream =
        twitterStream.map(new SentenceDetectorFunction(deSentenceModel))
        .timeWindowAll(Time.seconds(300))
        .apply(new SockeyeTranslateFunction());

    sentenceStream.writeAsCsv("/tmp/crapshit", FileSystem.WriteMode.OVERWRITE);


    // execute program
    env.execute("Executing Streaming Machine Translation");
  }

  private static class FilterEndpoint implements TwitterSource.EndpointInitializer, Serializable {
    private final List<String> tags;

    FilterEndpoint(final String... tags) {
      this.tags = Stream.of(tags).collect(Collectors.toList());
    }

    @Override
    public StreamingEndpoint createEndpoint() {
      StatusesFilterEndpoint ep = new StatusesFilterEndpoint();
      ep.trackTerms(tags);
      return ep;
    }
  }
}
