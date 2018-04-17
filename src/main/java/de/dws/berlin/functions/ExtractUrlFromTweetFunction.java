package de.dws.berlin.functions;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.dws.berlin.Tweet;
import org.apache.flink.api.common.functions.RichFlatMapFunction;

import org.apache.flink.util.Collector;

public class ExtractUrlFromTweetFunction extends RichFlatMapFunction<Tweet, List<String>> {

  @Override
  public void flatMap(Tweet value, Collector<List<String>> collector) {
    List<String> containedUrls = new ArrayList<>();
    String urlRegex = "((https?|ftp|gopher|telnet|file):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
    Pattern pattern = Pattern.compile(urlRegex, Pattern.CASE_INSENSITIVE);
    Matcher urlMatcher = pattern.matcher(value.getText());

    while (urlMatcher.find()) {
      containedUrls.add(value.getText().substring(urlMatcher.start(0), urlMatcher.end(0)));
    }

    collector.collect(containedUrls);
  }
}
