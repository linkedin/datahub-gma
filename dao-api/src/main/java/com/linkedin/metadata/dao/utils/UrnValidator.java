package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.exception.InvalidUrnException;
import com.linkedin.metadata.dao.exception.InvalidUrnException.Reason;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


/**
 * Validates Pegasus URN content before database writes.
 *
 * <p>Checks every key part of a URN (recursing into nested URNs) for:
 * <ul>
 *   <li>Blank or whitespace-only values</li>
 *   <li>Any whitespace character (space, NBSP, tab, newline)</li>
 *   <li>Unicode control (Cc) or format (Cf) characters (zero-width space, BOM, etc.)</li>
 * </ul>
 *
 * <p>Reads are intentionally not validated so historical bad rows remain accessible.
 */
@Slf4j
public final class UrnValidator {

  private static final String URN_PREFIX = "urn:";

  private UrnValidator() {
  }

  /**
   * Validates a Pegasus URN for write operations. Throws {@link InvalidUrnException} on the first
   * violation found.
   *
   * @param urn the URN to validate
   * @throws InvalidUrnException if any key part contains invalid content
   */
  public static void validateUrn(@Nonnull Urn urn) {
    String entityType = urn.getEntityType();
    List<String> parts = urn.getEntityKey().getParts();
    for (int i = 0; i < parts.size(); i++) {
      String part = parts.get(i);
      String path = "key[" + i + "]";
      validatePart(part, entityType, path);
    }
  }

  /**
   * Validates a URN and logs rejection details before rethrowing.
   *
   * @param operation label for the operation (e.g. "add", "create", "delete")
   * @param urn the URN to validate
   * @throws InvalidUrnException if any key part contains invalid content
   */
  public static void validateUrnForWrite(@Nonnull String operation, @Nonnull Urn urn) {
    try {
      validateUrn(urn);
    } catch (InvalidUrnException e) {
      logRejection(e, operation, urn.toString());
      throw e;
    }
  }

  private static void validatePart(@Nonnull String part, @Nonnull String entityType, @Nonnull String path) {
    if (isBlank(part)) {
      throw new InvalidUrnException(entityType, Reason.BLANK_FIELD, path, quote(part),
          String.format("URN %s is blank or whitespace-only at %s: %s codepoints=%s",
              entityType, path, quote(part), codepointList(part)));
    }

    if (containsAnyWhitespace(part)) {
      throw new InvalidUrnException(entityType, Reason.CONTAINS_WHITESPACE, path, quote(part),
          String.format("URN %s contains whitespace at %s: %s codepoints=%s",
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

  private static void validateNestedUrn(@Nonnull String urnString, @Nonnull String parentEntityType,
      @Nonnull String parentPath) {
    Urn nested;
    try {
      nested = Urn.createFromString(urnString);
    } catch (Exception e) {
      // If it can't be parsed as a URN, the string-level checks above already cover it.
      return;
    }
    String nestedEntityType = nested.getEntityType();
    List<String> parts = nested.getEntityKey().getParts();
    for (int i = 0; i < parts.size(); i++) {
      String nestedPath = parentPath + "." + nestedEntityType + ".key[" + i + "]";
      validatePart(parts.get(i), parentEntityType, nestedPath);
    }
  }

  static boolean isBlank(@Nonnull String value) {
    if (value.isEmpty()) {
      return true;
    }
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (!Character.isWhitespace(c) && !Character.isSpaceChar(c)) {
        return false;
      }
    }
    return true;
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

  private static void logRejection(@Nonnull InvalidUrnException e, @Nonnull String operation,
      @Nonnull String fullUrn) {
    log.warn("URN validation rejected {} for urn={} reason={} field={} value={}",
        operation, quote(fullUrn), e.getReason(), e.getFieldPath(), e.getRawValue());
  }
}
