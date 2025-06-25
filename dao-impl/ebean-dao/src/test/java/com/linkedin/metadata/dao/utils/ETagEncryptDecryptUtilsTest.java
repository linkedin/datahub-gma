package com.linkedin.metadata.dao.utils;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import org.testng.annotations.Test;

import static org.junit.Assert.*;


public class ETagEncryptDecryptUtilsTest {

  @Test
  public void testEncryptAndDecryptTimestamp()
      throws NoSuchPaddingException, IllegalBlockSizeException, NoSuchAlgorithmException, BadPaddingException,
             InvalidKeyException {
    long timestamp = System.currentTimeMillis();
    String encrypted = ETagEncryptDecryptUtils.encryptTimestamp(timestamp);

    long decrypted = ETagEncryptDecryptUtils.decryptTimestamp(encrypted);
    assertEquals(decrypted, timestamp);
  }

  @Test
  public void testEncryptTimestamp()
      throws NoSuchPaddingException, IllegalBlockSizeException, NoSuchAlgorithmException, BadPaddingException,
             InvalidKeyException {
    long timestamp = 1750796203701L;
    String encrypted = ETagEncryptDecryptUtils.encryptTimestamp(timestamp);

    assertEquals("KsFkRXtjaBGQf37HjdEjDQ==", encrypted);
  }

  @Test
  public void testDecryptTimestamp()
      throws NoSuchPaddingException, IllegalBlockSizeException, NoSuchAlgorithmException, BadPaddingException,
             InvalidKeyException {
    String encrypted = "KsFkRXtjaBGQf37HjdEjDQ==";
    long decrypted = ETagEncryptDecryptUtils.decryptTimestamp(encrypted);

    assertEquals(1750796203701L, decrypted);
  }
}
