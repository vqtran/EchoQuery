package echoquery.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.commons.codec.binary.Base64;

/**
 * Serializes any object into a base64 byte string and back.
 */

public final class Serializer {
  private Serializer() {}

  public static String serialize(Object obj) throws IOException {
    ByteArrayOutputStream b = new ByteArrayOutputStream();
    ObjectOutputStream o = new ObjectOutputStream(b);
    o.writeObject(obj);
    return Base64.encodeBase64String(b.toByteArray());
  }

  public static Object deserialize(String base64)
      throws IOException, ClassNotFoundException {
    ByteArrayInputStream b =
        new ByteArrayInputStream(Base64.decodeBase64(base64));
    ObjectInputStream o = new ObjectInputStream(b);
    return o.readObject();
  }
}