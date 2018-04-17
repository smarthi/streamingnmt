package de.dws.berlin;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

public class LanguageSelector implements OutputSelector<String> {
  private static String OTHER_LANGUAGES = "OTHER_LANGUAGES";

  private final Set<String> supportedLanguages;

  public LanguageSelector(String ... languages) {
    supportedLanguages = Stream.of(languages).collect(Collectors.toSet());
  }

  @Override
  public Iterable<String> select(String text) {
    if (supportedLanguages.contains(text))
      return Collections.singletonList(text);
    else
      return Collections.singletonList(OTHER_LANGUAGES);
  }
}

