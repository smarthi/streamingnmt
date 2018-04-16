package de.dws.berlin.functions;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.dws.berlin.Tweet;
import org.apache.flink.api.common.functions.RichFlatMapFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class ExtractUrlFromTweetFunction extends RichFlatMapFunction<Tweet, Tuple2<String, List<String>>> {

  @Override
  public void flatMap(Tweet value, Collector<Tuple2<String, List<String>>> collector) {
    List<String> urlList = new ArrayList<>();
    String tweet = value.getText();

    int startIndex = tweet.indexOf("https://", 0);
    while (startIndex >= 0 && startIndex < tweet.length()) {
      int endIndex = tweet.indexOf(" ", startIndex);
      urlList.add(tweet.substring(startIndex, endIndex));
      startIndex = endIndex;
    }
    collector.collect(new Tuple2<>(value.getId(), urlList));
  }
}
