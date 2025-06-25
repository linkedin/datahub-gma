package com.linkedin.metadata.dao.utils;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import javax.annotation.Nonnull;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;


/**
 * Utility class for IngestionAspectETag. E.g., encrypting and decrypting timestamps using AES encryption.
 */
public final class ETagUtils {

  public static final String AES = "AES";

  private ETagUtils() {
  }

  // 16-char key for AES-128
  private static final String SECRET_KEY = "9012312344567856";

  /**
   * Encrypts a timestamp using AES encryption.
   * @param timestamp Timestamp to encrypt
   * @return Encrypted timestamp as a Base64 encoded string
   */
  @Nonnull
  public static String encrypt(long timestamp)
      throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException, IllegalBlockSizeException,
             BadPaddingException {
    SecretKey secretKey = getSecretKey();

    Cipher cipher = Cipher.getInstance(AES);
    cipher.init(Cipher.ENCRYPT_MODE, secretKey);

    byte[] inputBytes = Long.toString(timestamp).getBytes();
    byte[] encrypted = cipher.doFinal(inputBytes);

    return Base64.getEncoder().encodeToString(encrypted);
  }

  /**
   * Decrypts a Base64 encoded encrypted timestamp string back to the original timestamp.
   * @param encrypted Base64 encoded encrypted timestamp string
   * @return Decrypted timestamp as a long
   */
  public static long decrypt(@Nonnull String encrypted)
      throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException, IllegalBlockSizeException,
             BadPaddingException {
    SecretKey secretKey = getSecretKey();

    Cipher cipher = Cipher.getInstance(AES);
    cipher.init(Cipher.DECRYPT_MODE, secretKey);

    byte[] decoded = Base64.getDecoder().decode(encrypted);
    byte[] decrypted = cipher.doFinal(decoded);

    return Long.parseLong(new String(decrypted));
  }

  private static SecretKey getSecretKey() {
    byte[] keyBytes = SECRET_KEY.getBytes();
    return new SecretKeySpec(keyBytes, AES);
  }

}
