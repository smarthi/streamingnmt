package de.dws.berlin.functions;

import de.dws.berlin.Tweet;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;

public class SentenceDetectorFunction extends RichMapFunction<Tweet, Tuple2<String,String[]>> {

  private transient SentenceDetector sentenceDetector;
  private final SentenceModel model;

  public SentenceDetectorFunction(final SentenceModel model) {
    this.model = model;
  }

  @Override
  public void open(Configuration parameters) {
    sentenceDetector = new SentenceDetectorME(model);
  }

  @Override
  public Tuple2<String,String[]> map(Tweet tweet) {
    String[] sentences = sentenceDetector.sentDetect(tweet.getText());
    return new Tuple2<>(tweet.getText(), sentences);
  }
}

