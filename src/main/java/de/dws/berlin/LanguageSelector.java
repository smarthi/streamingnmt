package de.dws.berlin;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;

public class LanguageSelector implements OutputSelector<Annotation> {
  private static String OTHER_LANGUAGES = "OTHER_LANGUAGES";

  private final Set<String> supportedLanguages;

  public LanguageSelector(String ... languages) {
    supportedLanguages = Stream.of(languages).collect(Collectors.toSet());
  }

  @Override
  public Iterable<String> select(Annotation annotation) {
    if (supportedLanguages.contains(annotation.getLanguage()))
      return Collections.singletonList(annotation.getLanguage());
    else
      return Collections.singletonList(OTHER_LANGUAGES);
  }
}

