create table metadata_id (
  namespace                     varchar(255) not null,
  id                            bigint not null,
  constraint uq_metadata_id_namespace_id unique (namespace,id)
);

create table metadata_aspect (
  urn                           varchar(500) not null,
  aspect                        varchar(200) not null,
  version                       bigint not null,
  metadata                      varchar(500) not null,
  createdon                     timestamp not null,
  createdby                     varchar(255) not null,
  createdfor                    varchar(255),
  constraint pk_metadata_aspect primary key (urn,aspect,version)
);

create table metadata_index (
  id                            bigint auto_increment not null,
  urn                           varchar(500) not null,
  aspect                        varchar(200) not null,
  path                          varchar(200) not null,
  longval                       bigint,
  stringval                     varchar(500),
  doubleval                     double,
  constraint pk_metadata_index primary key (id)
);

create index idx_long_val on metadata_index (aspect(100),path(100),longval,urn(100));
create index idx_string_val on metadata_index (aspect(100),path(100),stringval(100),urn(100));
create index idx_double_val on metadata_index (aspect(100),path(100),doubleval,urn(100));
create index idx_urn on metadata_index (urn(100));

CREATE TABLE IF NOT EXISTS metadata_relationship_belongsto (
    id BIGINT NOT NULL AUTO_INCREMENT,
    metadata JSON NOT NULL,
    source VARCHAR(500) NOT NULL,
    source_type VARCHAR(100) NOT NULL,
    destination VARCHAR(500) NOT NULL,
    destination_type VARCHAR(100) NOT NULL,
    lastmodifiedon DATETIME(6) NOT NULL,
    lastmodifiedby VARCHAR(255) NOT NULL,
    deleted_ts DATETIME(6) DEFAULT NULL,
    PRIMARY KEY (id)
);
