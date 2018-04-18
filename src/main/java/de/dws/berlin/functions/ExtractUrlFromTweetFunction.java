package de.dws.berlin.functions;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.dws.berlin.Tweet;
import de.dws.berlin.TweetURLMatcher;
import org.apache.flink.api.common.functions.RichFlatMapFunction;

import org.apache.flink.util.Collector;

public class ExtractUrlFromTweetFunction extends RichFlatMapFunction<Tweet, List<String>> {

  @Override
  public void flatMap(Tweet value, Collector<List<String>> collector) {
    List<String> containedUrls = new ArrayList<>();
    Matcher urlMatcher = TweetURLMatcher.checkUrlMatcherInTweet(value);
    while (urlMatcher.find()) {
      containedUrls.add(value.getText().substring(urlMatcher.start(0), urlMatcher.end(0)));
    }

    collector.collect(containedUrls);
  }
}
