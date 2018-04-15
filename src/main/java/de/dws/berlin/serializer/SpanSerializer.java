package de.dws.berlin.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import opennlp.tools.util.Span;

public class SpanSerializer extends Serializer<Span> {
  @Override
  public void write(Kryo kryo, Output output, Span object) {
    output.writeInt(object.getStart());
    output.writeInt(object.getEnd());
    output.writeString(object.getType());
  }

  @Override
  public Span read(Kryo kryo, Input input, Class<Span> type) {
    return new Span(input.readInt(), input.readInt(), input.readString());
  }
}
