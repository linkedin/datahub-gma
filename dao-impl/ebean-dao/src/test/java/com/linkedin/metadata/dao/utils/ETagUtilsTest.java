package com.linkedin.metadata.dao.utils;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import org.testng.annotations.Test;

import static org.junit.Assert.*;


public class ETagUtilsTest {

  @Test
  public void testEncryptAndDecrypt()
      throws NoSuchPaddingException, IllegalBlockSizeException, NoSuchAlgorithmException, BadPaddingException,
             InvalidKeyException {
    long timestamp = System.currentTimeMillis();
    String encrypted = ETagUtils.encrypt(timestamp);

    long decrypted = ETagUtils.decrypt(encrypted);
    assertEquals(decrypted, timestamp);
  }

  @Test
  public void testEncrypt()
      throws NoSuchPaddingException, IllegalBlockSizeException, NoSuchAlgorithmException, BadPaddingException,
             InvalidKeyException {
    long timestamp = 1750796203701L;
    String encrypted = ETagUtils.encrypt(timestamp);

    assertEquals("KsFkRXtjaBGQf37HjdEjDQ==", encrypted);
  }

  @Test
  public void testDecrypt()
      throws NoSuchPaddingException, IllegalBlockSizeException, NoSuchAlgorithmException, BadPaddingException,
             InvalidKeyException {
    String encrypted = "KsFkRXtjaBGQf37HjdEjDQ==";
    long decrypted = ETagUtils.decrypt(encrypted);

    assertEquals(1750796203701L, decrypted);
  }
}
