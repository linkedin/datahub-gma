package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.exception.InvalidUrnException;
import com.linkedin.metadata.dao.exception.InvalidUrnException.Reason;
import java.util.List;
import javax.annotation.Nonnull;


/**
 * Default URN validator that checks every key part of a URN for validity.
 *
 * <p>Recurses into nested URNs and rejects:
 * <ul>
 *   <li>Blank or whitespace-only values</li>
 *   <li>Any whitespace character (space, NBSP, tab, newline)</li>
 *   <li>Unicode control (Cc) or format (Cf) characters (zero-width space, BOM, etc.)</li>
 * </ul>
 */
public final class DefaultUrnValidator implements UrnValidator {

  private static final String URN_PREFIX = "urn:";

  @Override
  public void validate(@Nonnull Urn urn) {
    String entityType = urn.getEntityType();
    List<String> parts = urn.getEntityKey().getParts();
    for (int i = 0; i < parts.size(); i++) {
      String part = parts.get(i);
      String path = "key[" + i + "]";
      validatePart(part, entityType, path);
    }
  }

  private static void validatePart(@Nonnull String part, @Nonnull String entityType, @Nonnull String path) {
    if (part.isEmpty() || containsAnyWhitespace(part)) {
      throw new InvalidUrnException(entityType, Reason.CONTAINS_WHITESPACE, path, quote(part),
          String.format("URN %s is empty or contains whitespace at %s: %s codepoints=%s",
              entityType, path, quote(part), codepointList(part)));
    }

    if (containsControlCharacter(part)) {
      throw new InvalidUrnException(entityType, Reason.CONTROL_CHARACTER, path, quote(part),
          String.format("URN %s contains control/format character at %s: %s codepoints=%s",
              entityType, path, quote(part), codepointList(part)));
    }

    if (part.startsWith(URN_PREFIX)) {
      validateNestedUrn(part, entityType, path);
    }
  }

  private static void validateNestedUrn(@Nonnull String urnString, @Nonnull String entityType,
      @Nonnull String parentPath) {
    Urn nested;
    try {
      nested = Urn.createFromString(urnString);
    } catch (Exception e) {
      throw new InvalidUrnException(entityType, Reason.INVALID_NESTED_URN, parentPath, quote(urnString),
          String.format("URN %s has unparseable nested URN at %s: %s error=%s",
              entityType, parentPath, quote(urnString), e.getMessage()));
    }
    String nestedEntityType = nested.getEntityType();
    List<String> parts = nested.getEntityKey().getParts();
    for (int i = 0; i < parts.size(); i++) {
      String nestedPath = parentPath + "." + nestedEntityType + ".key[" + i + "]";
      validatePart(parts.get(i), nestedEntityType, nestedPath);
    }
  }

  static boolean containsAnyWhitespace(@Nonnull String value) {
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (Character.isWhitespace(c) || Character.isSpaceChar(c)) {
        return true;
      }
    }
    return false;
  }

  static boolean containsControlCharacter(@Nonnull String value) {
    for (int i = 0; i < value.length(); ) {
      int cp = value.codePointAt(i);
      int type = Character.getType(cp);
      if (type == Character.CONTROL || type == Character.FORMAT) {
        return true;
      }
      i += Character.charCount(cp);
    }
    return false;
  }

  /**
   * Escapes all non-printable-ASCII characters as {@code \\uXXXX} to prevent log injection.
   */
  static String quote(@Nonnull String value) {
    StringBuilder sb = new StringBuilder(value.length() + 8);
    sb.append('"');
    for (int i = 0; i < value.length(); ) {
      int cp = value.codePointAt(i);
      if (cp == '\\') {
        sb.append("\\\\");
      } else if (cp == '"') {
        sb.append("\\\"");
      } else if (cp >= 0x20 && cp < 0x7F) {
        sb.append((char) cp);
      } else {
        sb.append(String.format("\\u%04X", cp));
      }
      i += Character.charCount(cp);
    }
    sb.append('"');
    return sb.toString();
  }

  static String codepointList(@Nonnull String value) {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    for (int i = 0; i < value.length(); ) {
      int cp = value.codePointAt(i);
      if (sb.length() > 1) {
        sb.append(',');
      }
      sb.append(String.format("U+%04X", cp));
      i += Character.charCount(cp);
    }
    sb.append(']');
    return sb.toString();
  }
}
