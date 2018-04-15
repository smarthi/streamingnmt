package de.dws.berlin;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import opennlp.tools.util.Span;

public class Annotation implements Serializable {

  private String id;
  private String sofa;
  private String language;
  private String topic;

  private String sentiment;
  private String sentimentSum;
  private String sentimentOld;

  private final Map<String, String> properties = Collections.emptyMap();

  private Span headline;
  private Span[] sentences;
  private Span[][] tokens;
  private String[][] personMentions;
  private String[][] personGenders;
  private String[][] organizationMentions;
  private String[][] locationMentions;

  private String[][] pos;
  private Span[][] chunks;

  public Annotation() {
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setSofa(String sofa) {
    this.sofa = sofa;
  }

  public String getId() {
    return id;
  }

  public String getSofa() {
    return sofa;
  }

  public Span getHeadline() {
    return headline;
  }

  public void setHeadline(Span headline) {
    this.headline = headline;
  }

  public String getLanguage() {
    return language;
  }

  public void setLanguage(String language) {
    this.language = language;
  }

  public String[][] getPos() {
    return pos;
  }

  public void setPos(String[][] pos) {
    this.pos = pos;
  }

  public String putProperty(String key, String value) {
    return properties.put(key, value);
  }

  public String getProperty(String key) {
    return properties.get(key);
  }

  public Span[] getSentences() {
    if (sentences != null) {
      return sentences;
    } else {
      return new Span[0];
    }
  }

  public Map<String, String> getProperties() {
    return Collections.unmodifiableMap(properties);
  }

  public String[][] getPersonMentions() {
    return personMentions;
  }

  public void setPersonMentions(String[][] personMentions) {
    this.personMentions = personMentions;
  }

  public String[][] getOrganizationMentions() {
    return organizationMentions;
  }

  public void setOrganizationMentions(String[][] organizationMentions) {
    this.organizationMentions = organizationMentions;
  }

  public String[][] getLocationMentions() {
    return locationMentions;
  }

  public void setLocationMentions(String[][] locationMentions) {
    this.locationMentions = locationMentions;
  }

  public void setChunks(Span[][] chunks) {
    this.chunks = chunks;
  }

  public void setSentences(Span[] sentences) {
    this.sentences = sentences;

    tokens = new Span[sentences.length][];
    pos = new String[sentences.length][];
    personMentions = new String[sentences.length][];
    organizationMentions = new String[sentences.length][];
    locationMentions = new String[sentences.length][];
    chunks = new Span[sentences.length][];
  }

  public Span[][] getTokens() {
    return tokens;
  }

  public void setTokens(Span[][] tokens) {
    this.tokens = tokens;
  }

  @Override
  public String toString() {
    return getLanguage() + " : " + getSofa().substring(getHeadline().getStart(), getHeadline().getEnd());
  }

  public Span[][] getChunks() {
    return chunks;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getSentiment() {
    return sentiment;
  }

  public void setSentiment(String sentiment) {
    this.sentiment = sentiment;
  }

  public String[][] getPersonGenders() {
    return personGenders;
  }

  public void setPersonGenders(String[][] personGenders) {
    this.personGenders = personGenders;
  }

  public String getSentimentSum() {
    return sentimentSum;
  }

  public void setSentimentSum(String sentimentSum) {
    this.sentimentSum = sentimentSum;
  }

  public String getSentimentOld() {
    return sentimentOld;
  }

  public void setSentimentOld(String sentimentOld) {
    this.sentimentOld = sentimentOld;
  }
}
