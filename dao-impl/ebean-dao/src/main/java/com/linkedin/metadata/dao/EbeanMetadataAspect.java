package com.linkedin.metadata.dao;

import io.ebean.Model;
import io.ebean.annotation.Index;
import java.sql.Timestamp;
import javax.annotation.Nullable;
import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Transient;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;


/**
 * Schema definition for the metadata aspect table.
 */
@Getter
@Setter
@Entity
@Table(name = "metadata_aspect")
public class EbeanMetadataAspect extends Model {

  private static final long serialVersionUID = 1L;

  public static final String ALL_COLUMNS = "*";
  public static final String KEY_ID = "key";
  public static final String URN_COLUMN = "urn";
  public static final String ASPECT_COLUMN = "aspect";
  public static final String VERSION_COLUMN = "version";
  public static final String METADATA_COLUMN = "metadata";
  public static final String CREATED_ON_COLUMN = "createdOn";
  public static final String CREATED_BY_COLUMN = "createdBy";
  public static final String CREATED_FOR_COLUMN = "createdFor";

  /**
   * Key for an aspect in the table.
   */
  @Embeddable
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  @EqualsAndHashCode
  public static class PrimaryKey {

    private static final long serialVersionUID = 1L;

    @NonNull
    @Index
    @Column(name = URN_COLUMN, length = 500, nullable = false)
    private String urn;

    @NonNull
    @Index
    @Column(name = ASPECT_COLUMN, length = 200, nullable = false)
    private String aspect;

    @Index
    @Column(name = VERSION_COLUMN, nullable = false)
    private long version;
  }

  @NonNull
  @EmbeddedId
  @Index
  protected PrimaryKey key;

  @Lob
  @Column(name = METADATA_COLUMN, nullable = false)
  protected String metadata;

  @NonNull
  @Column(name = CREATED_ON_COLUMN, nullable = false)
  private Timestamp createdOn;

  @NonNull
  @Column(name = CREATED_BY_COLUMN, nullable = false)
  private String createdBy;

  @Column(name = CREATED_FOR_COLUMN, nullable = true)
  private String createdFor;

  // this column doesn't exist in the old schema
  // in the new schema, it's part of the aspect json
  @Nullable
  @Transient
  private Long emitTime;

  // this column doesn't exist in the old schema
  // in the new schema, it's part of the aspect json
  @Nullable
  @Transient
  private String emitter;

  // TODO (@jphui) META-18962 De-deduplicity investigation
  // @SneakyThrows
  // @Override
  // public boolean equals(Object o) {
  //   if (o == null) {
  //     return false;
  //   }
  //   if (o.getClass() != this.getClass()) {
  //     return false;
  //   }
  //   EbeanMetadataAspect other = (EbeanMetadataAspect) o;

  //   boolean primitiveEqualityCheck = this.key.equals(other.key)
  //       // either both metadata fields are null or both are equal (will check non-null equality after)
  //       && ((this.metadata == null && other.metadata == null) || (this.metadata != null && other.metadata != null))
  //       && Math.abs(this.createdOn.getTime() - other.getCreatedOn().getTime()) < 1000 // timestamps are considered equal if within 1s of each other
  //       && this.createdBy.equals(other.getCreatedBy())
  //       // either both createdFor fields are null or both are equal (need to check this.createdFor != null to avoid NPE)
  //       && ((this.createdFor == null && other.getCreatedFor() == null) || (this.createdFor != null && this.createdFor.equals(other.getCreatedFor())));
  //   if (!primitiveEqualityCheck) {
  //     return false;
  //   }

  //   JSONParser parser = new JSONParser();
  //   JSONObject thisMetadata = (JSONObject) parser.parse(this.metadata);
  //   JSONObject otherMetadata = (JSONObject) parser.parse(other.metadata);
  //   return thisMetadata.equals(otherMetadata);
  // }

  // @Override
  // public int hashCode() {
  //   return super.hashCode();
  // }

  @Override
  public String toString() {
    final String str = "EbeanMetadataAspect: {key: <urn:%s, aspect:%s, version:%s>, createdOn: %s, createdBy: %s, createdFor: %s, metadata: %s}";
    return String.format(str, key.getUrn(), key.getAspect(), key.getVersion(), createdOn, createdBy, createdFor, metadata);
  }
}
