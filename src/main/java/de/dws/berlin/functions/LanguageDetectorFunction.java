package de.dws.berlin.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import opennlp.tools.langdetect.LanguageDetector;
import opennlp.tools.langdetect.LanguageDetectorME;
import opennlp.tools.langdetect.LanguageDetectorModel;

public class LanguageDetectorFunction extends RichMapFunction<String,String> {

  private transient LanguageDetector languageDetector;

  @Override
  public void open(Configuration parameters) throws Exception {
    languageDetector = new LanguageDetectorME(new LanguageDetectorModel(
        LanguageDetectorFunction.class.getResource("/opennlp-models/langdetect-183.bin")));
  }

  @Override
  public String map(String text) {
    return languageDetector.predictLanguage(text).getLang();
  }
}
