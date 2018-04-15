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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import de.dws.berlin.serializer.AnnotationSerializer;
import de.dws.berlin.serializer.SpanSerializer;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoop.shaded.org.codehaus.jackson.JsonNode;
import org.apache.flink.hadoop.shaded.org.codehaus.jackson.map.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import opennlp.tools.chunker.ChunkerModel;
import opennlp.tools.doccat.DoccatModel;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.postag.POSModel;
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

  private static SentenceModel engSentenceModel;
  private static TokenizerModel engTokenizerModel;
  private static POSModel engPosModel;
  private static ChunkerModel engChunkModel;
  private static TokenNameFinderModel engNerPersonModel;
  private static DoccatModel engDoccatModel;

  private static void initializeModels() throws IOException {
    engSentenceModel = new SentenceModel(StreamingNmt.class.getResource("/opennlp-models/en-sent.bin"));
    engTokenizerModel = new TokenizerModel(StreamingNmt.class.getResource("/opennlp-models/en-token.bin"));
    engPosModel= new POSModel(StreamingNmt.class.getResource("/opennlp-models/en-pos-perceptron.bin"));
    engChunkModel = new ChunkerModel(StreamingNmt.class.getResource("/opennlp-models/en-chunker.bin"));
    engNerPersonModel = new TokenNameFinderModel(StreamingNmt.class.getResource("/opennlp-models/en-ner.bin"));
    engDoccatModel = new DoccatModel(StreamingNmt.class.getResource("/opennlp-models/en-doccat.bin"));
  }

  public static void main(String[] args) throws Exception {

    initializeModels();

    ParameterTool parameterTool = ParameterTool.fromArgs(args);

    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment()
            .setParallelism(parameterTool.getInt("parallelism", 1))
            .setMaxParallelism(10);

    env.getConfig().enableObjectReuse();
    env.getConfig().registerTypeWithKryoSerializer(Annotation.class, AnnotationSerializer.class);
    env.getConfig().registerTypeWithKryoSerializer(Span.class, SpanSerializer.class);
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

    // twitter credentials and source
    Properties props = new Properties();
    props.load(StreamingNmt.class.getResourceAsStream("/twitter.properties"));
    TwitterSource twitterSource = new TwitterSource(props);
    twitterSource.setCustomEndpointInitializer(new StreamingNmt.FilterEndpoint(""));

    // Create a DataStream from TwitterSource filtered by deleted tweets
    DataStream<String> twitterStream = env.addSource(new TwitterSource(props))
        .filter((FilterFunction<String>) value -> value.contains("created-at"))
        .map(new StreamingNmt.JsonConverter());

    twitterStream.print();


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

  private static class JsonConverter extends RichMapFunction<String,String> {
    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      mapper = new ObjectMapper();
    }

    @Override
    public String map(String value) throws Exception {
      JsonNode tweet = mapper.readValue(value, JsonNode.class);
      return tweet.get("text").asText();
    }
  }
}
