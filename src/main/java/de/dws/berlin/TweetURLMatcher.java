package de.dws.berlin;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TweetURLMatcher {

  public static boolean checkUrlInTweet(Tweet tweet) {
    return checkUrlMatcherInTweet(tweet).find();
  }

  public static Matcher checkUrlMatcherInTweet(Tweet tweet) {
    String urlRegex = "((https?|ftp|gopher|telnet|file):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
    Pattern pattern = Pattern.compile(urlRegex, Pattern.CASE_INSENSITIVE);
    return pattern.matcher(tweet.getText());
  }
}
