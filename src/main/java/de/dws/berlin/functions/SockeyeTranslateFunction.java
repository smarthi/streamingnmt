package de.dws.berlin.functions;

import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class SockeyeTranslateFunction extends RichAllWindowFunction<String, String, TimeWindow> {

  @Override
  public void apply(TimeWindow timeWindow, Iterable<String> iterable,
                    Collector<String> collector) throws Exception {

  }
}
