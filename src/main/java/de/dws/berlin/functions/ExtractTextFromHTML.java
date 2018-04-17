package de.dws.berlin.functions;

import java.io.IOException;
import java.util.List;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class ExtractTextFromHTML extends RichFlatMapFunction<List<String>, String> {
  @Override
  public void flatMap(List<String> urls, Collector<String> collector)  {
    Document document;
    String text;
    if (urls.size() > 0) {
      for (String url : urls) {
        try {
          System.out.println(url);
          Connection.Response response = Jsoup.connect(url).followRedirects(true).execute();
          text = Jsoup.parse(response.parse().html()).body().text();
          collector.collect(text);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
