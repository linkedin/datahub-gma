DROP TABLE IF EXISTS metadata_relationship_belongsto;
DROP TABLE IF EXISTS metadata_relationship_reportsto;
DROP TABLE IF EXISTS metadata_relationship_ownedby;
DROP TABLE IF EXISTS metadata_relationship_pairswith;
DROP TABLE IF EXISTS metadata_relationship_versionof;
DROP TABLE IF EXISTS metadata_relationship_consumefrom;
DROP TABLE IF EXISTS metadata_entity_foo;
DROP TABLE IF EXISTS metadata_entity_bar;

CREATE TABLE IF NOT EXISTS metadata_relationship_belongsto (
                                                               id BIGINT NOT NULL AUTO_INCREMENT,
                                                               metadata LONGTEXT NOT NULL,
                                                               source VARCHAR(1000) NOT NULL,
    source_type VARCHAR(100) NOT NULL,
    destination VARCHAR(1000) NOT NULL,
    destination_type VARCHAR(100) NOT NULL,
    lastmodifiedon TIMESTAMP NOT NULL,
    lastmodifiedby VARCHAR(255) NOT NULL,
    deleted_ts DATETIME(6) DEFAULT NULL,
    PRIMARY KEY (id)
    );

CREATE TABLE IF NOT EXISTS metadata_relationship_reportsto (
                                                               id BIGINT NOT NULL AUTO_INCREMENT,
                                                               metadata JSON NOT NULL,
                                                               source VARCHAR(1000) NOT NULL,
    source_type VARCHAR(100) NOT NULL,
    destination VARCHAR(1000) NOT NULL,
    destination_type VARCHAR(100) NOT NULL,
    lastmodifiedon TIMESTAMP NOT NULL,
    lastmodifiedby VARCHAR(255) NOT NULL,
    deleted_ts DATETIME(6) DEFAULT NULL,
    PRIMARY KEY (id)
    );

CREATE TABLE IF NOT EXISTS metadata_relationship_ownedby (
                                                               id BIGINT NOT NULL AUTO_INCREMENT,
                                                               metadata JSON NOT NULL,
                                                               source VARCHAR(1000) NOT NULL,
    source_type VARCHAR(100) NOT NULL,
    destination VARCHAR(1000) NOT NULL,
    destination_type VARCHAR(100) NOT NULL,
    lastmodifiedon TIMESTAMP NOT NULL,
    lastmodifiedby VARCHAR(255) NOT NULL,
    deleted_ts DATETIME(6) DEFAULT NULL,
    PRIMARY KEY (id)
    );

CREATE TABLE IF NOT EXISTS metadata_relationship_pairswith (
                                                               id BIGINT NOT NULL AUTO_INCREMENT,
                                                               metadata JSON NOT NULL,
                                                               source VARCHAR(1000) NOT NULL,
    source_type VARCHAR(100) NOT NULL,
    destination VARCHAR(1000) NOT NULL,
    destination_type VARCHAR(100) NOT NULL,
    lastmodifiedon TIMESTAMP NOT NULL,
    lastmodifiedby VARCHAR(255) NOT NULL,
    deleted_ts DATETIME(6) DEFAULT NULL,
    PRIMARY KEY (id)
    );

CREATE TABLE IF NOT EXISTS metadata_relationship_versionof (
                                                               id BIGINT NOT NULL AUTO_INCREMENT,
                                                               metadata JSON NOT NULL,
                                                               source VARCHAR(1000) NOT NULL,
    source_type VARCHAR(100) NOT NULL,
    destination VARCHAR(1000) NOT NULL,
    destination_type VARCHAR(100) NOT NULL,
    lastmodifiedon TIMESTAMP NOT NULL,
    lastmodifiedby VARCHAR(255) NOT NULL,
    deleted_ts DATETIME(6) DEFAULT NULL,
    PRIMARY KEY (id)
    );

CREATE TABLE IF NOT EXISTS metadata_relationship_consumefrom (
                                                                 id BIGINT NOT NULL AUTO_INCREMENT,
                                                                 metadata JSON NOT NULL,
                                                                 source VARCHAR(1000) NOT NULL,
    source_type VARCHAR(100) NOT NULL,
    destination VARCHAR(1000) NOT NULL,
    destination_type VARCHAR(100) NOT NULL,
    lastmodifiedon TIMESTAMP NOT NULL,
    lastmodifiedby VARCHAR(255) NOT NULL,
    deleted_ts DATETIME(6) DEFAULT NULL,
    PRIMARY KEY (id)
    );

-- initialize foo entity table
CREATE TABLE IF NOT EXISTS metadata_entity_foo (
    urn VARCHAR(100) NOT NULL,
    lastmodifiedon TIMESTAMP NOT NULL,
    lastmodifiedby VARCHAR(255) NOT NULL,
    createdfor VARCHAR(255),
    CONSTRAINT pk_metadata_entity_foo PRIMARY KEY (urn)
    );

-- initialize foo entity table
CREATE TABLE IF NOT EXISTS metadata_entity_bar (
    urn VARCHAR(100) NOT NULL,
    lastmodifiedon TIMESTAMP NOT NULL,
    lastmodifiedby VARCHAR(255) NOT NULL,
    createdfor VARCHAR(255),
    CONSTRAINT pk_metadata_entity_bar PRIMARY KEY (urn)
    );

-- add foo aspect to foo entity
ALTER TABLE metadata_entity_foo ADD a_aspectfoo JSON;

-- add foo aspect to foo entity
ALTER TABLE metadata_entity_foo ADD a_aspectbar JSON;

-- add new index virtual column 'value'
ALTER TABLE metadata_entity_foo ADD COLUMN i_aspectfoo0value VARCHAR(255)
    GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(a_aspectfoo, '$.aspect.value')));

-- add new index virtual column 'value'
ALTER TABLE metadata_entity_foo ADD COLUMN i_aspectbar0value VARCHAR(255)
    GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(a_aspectbar, '$.aspect.value')));

-- add foo aspect to bar entity
ALTER TABLE metadata_entity_bar ADD a_aspectfoo JSON;

-- add new index virtual column 'value'
ALTER TABLE metadata_entity_bar ADD COLUMN i_aspectfoo0value VARCHAR(255)
    GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(a_aspectfoo, '$.aspect.value')));

-- add new virtual column 'value' to relationship table
ALTER TABLE metadata_relationship_consumefrom ADD COLUMN metadata0environment VARCHAR(255)
    GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.environment')));