package de.dws.berlin.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import de.dws.berlin.thrift.Translate;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;


public class SockeyeTranslateFunction extends RichAllWindowFunction<Tuple2<String, String[]>, Tuple2<String,String>, TimeWindow> {

  private String host;
  private int port;
  private TTransport tTransport;
  private TProtocol protocol;
  private Translate.Client translateClient;

  @Override
  public void open(Configuration parameters) throws Exception {
    Properties props = new Properties();
    props.load(SockeyeTranslateFunction.class.getResourceAsStream("/thrift.properties"));
    host = props.getProperty("thrift.server");
    port = Integer.valueOf(props.getProperty("thrift.port"));
    tTransport = new TSocket(host, port);

    tTransport.open();

    TProtocol protocol = new TBinaryProtocol(tTransport);
    translateClient = new Translate.Client(protocol);
  }

  @Override
  public void apply(TimeWindow window, Iterable<Tuple2<String, String[]>> iterable,
                    Collector<Tuple2<String, String>> collector) throws Exception {

    List<String> sentencesList = new ArrayList<>();

    for (Tuple2<String,String[]> sentences : iterable) {
      sentencesList.addAll(Arrays.asList(sentences.f1));
    }

    Collections.sort(sentencesList);

    for (String sentence : sentencesList) {
      String translatedString = translateClient.translate(sentence);
      System.out.println(sentence + " :: Translation =  " + translatedString.replaceAll("@@ ", ""));
      collector.collect(new Tuple2<>(sentence, translatedString.replaceAll("@@ ", "")));
    }
  }
}