package de.dws.berlin.serializer;

import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import de.dws.berlin.Annotation;

import opennlp.tools.util.Span;

public class AnnotationSerializer extends Serializer<Annotation> {

  @Override
  public void write(Kryo kryo, Output output, Annotation ann) {
    output.writeString(ann.getId());
    output.writeString(ann.getLanguage());
    kryo.writeObject(output, ann.getHeadline());
    output.writeString(ann.getSofa());

    output.writeInt(ann.getSentences().length);
    for (int i=0; i<ann.getSentences().length; i++) {
      kryo.writeObject(output, ann.getSentences()[i]);
    }

    output.writeInt(ann.getTokens().length);
    for (int i=0; i<ann.getTokens().length; i++) {
      output.writeInt(ann.getTokens()[i].length);
      for (int j=0; j<ann.getTokens()[i].length; j++)
        kryo.writeObject(output, ann.getTokens()[i][j]);
    }

    output.writeInt(ann.getChunks().length);
    for (int i=0; i<ann.getChunks().length; i++) {
      output.writeInt(ann.getChunks()[i].length);
      for (int j=0; j<ann.getChunks()[i].length; j++)
        kryo.writeObject(output, ann.getChunks()[i][j]);
    }

    output.writeInt(ann.getPos().length);
    for (int i=0; i<ann.getPos().length; i++) {
      output.writeInt(ann.getPos()[i].length);
      for (int j=0; j<ann.getPos()[i].length; j++)
        output.writeString(ann.getPos()[i][j]);
    }

    output.writeInt(ann.getPersonMentions().length);
    for (int i=0; i<ann.getPersonMentions().length; i++) {
      output.writeInt(ann.getPersonMentions()[i].length);
      for (int j=0; j<ann.getPersonMentions()[i].length; j++)
        output.writeString(ann.getPersonMentions()[i][j]);
    }

    output.writeInt(ann.getOrganizationMentions().length);
    for (int i=0; i<ann.getOrganizationMentions().length; i++) {
      output.writeInt(ann.getOrganizationMentions()[i].length);
      for (int j=0; j<ann.getOrganizationMentions()[i].length; j++)
        output.writeString(ann.getOrganizationMentions()[i][j]);
    }

    output.writeInt(ann.getLocationMentions().length);
    for (int i=0; i<ann.getLocationMentions().length; i++) {
      output.writeInt(ann.getLocationMentions()[i].length);
      for (int j=0; j<ann.getLocationMentions()[i].length; j++)
        output.writeString(ann.getLocationMentions()[i][j]);
    }

    output.writeInt(ann.getProperties().size());
    for(Map.Entry<String,String> entry : ann.getProperties().entrySet()) {
      output.writeString(entry.getKey());
      output.writeString(entry.getValue());
    }

    output.writeString(ann.getTopic());

  }

  @Override
  public Annotation read(Kryo kryo, Input input, Class<Annotation> type) {

    Annotation annotation = new Annotation();

    annotation.setId(input.readString());
    annotation.setLanguage(input.readString());
    annotation.setHeadline(kryo.readObject(input, Span.class));
    annotation.setSofa(input.readString());

    Span[] sentences = new Span[input.readInt()];
    for (int i=0; i<sentences.length; i++) {
      sentences[i] = kryo.readObject(input, Span.class);
    }
    annotation.setSentences(sentences);

    Span[][] tokens = new Span[input.readInt()][];
    for (int i=0; i<tokens.length; i++) {
      tokens[i] = new Span[input.readInt()];
      for (int j=0; j<tokens[i].length; j++)
        tokens[i][j] = kryo.readObject(input, Span.class);
    }
    annotation.setTokens(tokens);

    Span[][] chunks = new Span[input.readInt()][];
    for (int i=0; i<chunks.length; i++) {
      chunks[i] = new Span[input.readInt()];
      for (int j=0; j<chunks[i].length; j++)
        chunks[i][j] = kryo.readObject(input, Span.class);
    }
    annotation.setChunks(chunks);

    String[][] pos = new String[input.readInt()][];
    for (int i=0; i<pos.length; i++) {
      pos[i] = new String[input.readInt()];
      for (int j=0; j<pos[i].length; j++)
        pos[i][j] = input.readString();
    }
    annotation.setPos(pos);

    String[][] per = new String[input.readInt()][];
    for (int i=0; i<per.length; i++) {
      per[i] = new String[input.readInt()];
      for (int j=0; j<per[i].length; j++)
        per[i][j] = input.readString();
    }
    annotation.setPersonMentions(per);

    String[][] org = new String[input.readInt()][];
    for (int i=0; i<org.length; i++) {
      org[i] = new String[input.readInt()];
      for (int j=0; j<org[i].length; j++)
        org[i][j] = input.readString();
    }
    annotation.setOrganizationMentions(org);

    String[][] loc = new String[input.readInt()][];
    for (int i=0; i<loc.length; i++) {
      loc[i] = new String[input.readInt()];
      for (int j=0; j<loc[i].length; j++)
        loc[i][j] = input.readString();
    }
    annotation.setLocationMentions(loc);

    for (int i=0; i<input.readInt(); i++) {
      annotation.putProperty(input.readString(), input.readString());
    }

    annotation.setTopic(input.readString());

    return annotation;
  }
}