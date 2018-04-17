package de.dws.berlin.functions;

import de.dws.berlin.Annotation;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;

public class SentenceDetectorFunction extends RichMapFunction<String, String[]> {

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
  public String[] map(String text) {
    return sentenceDetector.sentDetect(text);

  }
}

