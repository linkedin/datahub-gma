package com.linkedin.metadata.dao.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;


/**
 * Covers the empty-index-metadata warning. Deliberately a pure unit test with no database: the
 * behaviour being pinned is what the DAO does when {@code information_schema.STATISTICS} yields
 * nothing, which cannot be provoked against a working embedded instance.
 */
public class SchemaValidatorUtilWarningTest {

  private static final class CapturingAppender extends AbstractAppender {
    private final List<String> _warnings = Collections.synchronizedList(new ArrayList<>());

    private CapturingAppender() {
      super("capturing", null, PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
      if (event.getLevel() == Level.WARN) {
        _warnings.add(event.getMessage().getFormattedMessage());
      }
    }
  }

  private CapturingAppender _appender;
  private Logger _logger;
  private Level _originalLevel;

  @BeforeMethod
  public void setUp() {
    _appender = new CapturingAppender();
    _appender.start();
    _logger = (Logger) LogManager.getLogger(SchemaValidatorUtil.class);
    _originalLevel = _logger.getLevel();
    // Log4j2 defaults to ERROR when no configuration file is present, which would filter the warning.
    Configurator.setLevel(SchemaValidatorUtil.class.getName(), Level.WARN);
    _logger.addAppender(_appender);
  }

  @AfterMethod
  public void tearDown() {
    if (_logger != null && _appender != null) {
      _logger.removeAppender(_appender);
      _appender.stop();
      Configurator.setLevel(SchemaValidatorUtil.class.getName(), _originalLevel);
    }
  }

  @Test
  public void testWarnsWhenNoIndexMetadataIsVisible() {
    SchemaValidatorUtil.warnIfNoIndexMetadata("metadata_relationship_downstreamof", Collections.emptySet());

    assertEquals(1, _appender._warnings.size());
    String warning = _appender._warnings.get(0);
    // The table name is the actionable part: it is what an operator greps for.
    assertTrue(warning.contains("metadata_relationship_downstreamof"));
    // Name the consequence, so the log explains why queries got slower rather than only what was missing.
    assertTrue(warning.contains("FORCE INDEX"));
  }

  /**
   * A table that reports any index at all is healthy. Every existing InnoDB table reports at least
   * PRIMARY, so a non-empty set never warrants the warning and would otherwise be constant noise on
   * every cache refresh.
   */
  @Test
  public void testDoesNotWarnWhenIndexMetadataIsPresent() {
    Set<String> indexes = new HashSet<>();
    indexes.add("primary");

    SchemaValidatorUtil.warnIfNoIndexMetadata("metadata_relationship_downstreamof", indexes);

    assertTrue(_appender._warnings.isEmpty());
  }
}
