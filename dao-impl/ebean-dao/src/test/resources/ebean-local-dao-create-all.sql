DROP TABLE IF EXISTS metadata_entity_foo;
DROP TABLE IF EXISTS metadata_entity_foo_test;
DROP TABLE IF EXISTS metadata_entity_bar;
DROP TABLE IF EXISTS metadata_entity_burger;
DROP TABLE IF EXISTS metadata_aspect;
DROP TABLE IF EXISTS metadata_id;
DROP TABLE IF EXISTS metadata_index;
DROP TABLE IF EXISTS metadata_relationship_belongsto;
DROP TABLE IF EXISTS metadata_relationship_belongstov2;

-- initialize foo entity table
CREATE TABLE IF NOT EXISTS metadata_entity_foo (
    urn VARCHAR(100) NOT NULL,
    lastmodifiedon TIMESTAMP NOT NULL,
    lastmodifiedby VARCHAR(255) NOT NULL,
    createdfor VARCHAR(255),
    deleted_ts datetime(6) DEFAULT NULL,
    CONSTRAINT pk_metadata_entity_foo PRIMARY KEY (urn)
);

-- initialize foo entity test table
CREATE TABLE IF NOT EXISTS metadata_entity_foo_test (
    urn VARCHAR(100) NOT NULL,
    lastmodifiedon TIMESTAMP NOT NULL,
    lastmodifiedby VARCHAR(255) NOT NULL,
    createdfor VARCHAR(255),
    deleted_ts datetime(6) DEFAULT NULL,
    CONSTRAINT pk_metadata_entity_foo_test PRIMARY KEY (urn)
);

-- initialize bar entity table
CREATE TABLE IF NOT EXISTS metadata_entity_bar (
    urn VARCHAR(100) NOT NULL,
    lastmodifiedon TIMESTAMP NOT NULL,
    lastmodifiedby VARCHAR(255) NOT NULL,
    createdfor VARCHAR(255),
    deleted_ts datetime(6) DEFAULT NULL,
    CONSTRAINT pk_metadata_entity_bar PRIMARY KEY (urn)
);

-- initialize bar entity table
CREATE TABLE IF NOT EXISTS metadata_entity_burger (
    urn VARCHAR(100) NOT NULL,
    lastmodifiedon TIMESTAMP NOT NULL,
    lastmodifiedby VARCHAR(255) NOT NULL,
    createdfor VARCHAR(255),
    deleted_ts datetime(6) DEFAULT NULL,
    CONSTRAINT pk_metadata_entity_burger PRIMARY KEY (urn)
);

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
    aspect VARCHAR(200) DEFAULT NULL, -- should be NOT NULL in production use cases
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS metadata_relationship_belongstov2 (
    id BIGINT NOT NULL AUTO_INCREMENT,
    metadata LONGTEXT NOT NULL,
    source VARCHAR(1000) NOT NULL,
    source_type VARCHAR(100) NOT NULL,
    destination VARCHAR(1000) NOT NULL,
    destination_type VARCHAR(100) NOT NULL,
    lastmodifiedon TIMESTAMP NOT NULL,
    lastmodifiedby VARCHAR(255) NOT NULL,
    deleted_ts DATETIME(6) DEFAULT NULL,
    aspect VARCHAR(200) DEFAULT NULL, -- should be NOT NULL in production use cases
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS metadata_relationship_reportsto (
    id BIGINT NOT NULL AUTO_INCREMENT,
    metadata LONGTEXT NOT NULL,
    source VARCHAR(1000) NOT NULL,
    source_type VARCHAR(100) NOT NULL,
    destination VARCHAR(1000) NOT NULL,
    destination_type VARCHAR(100) NOT NULL,
    lastmodifiedon TIMESTAMP NOT NULL,
    lastmodifiedby VARCHAR(255) NOT NULL,
    deleted_ts DATETIME(6) DEFAULT NULL,
    aspect VARCHAR(200) DEFAULT NULL, -- should be NOT NULL in production use cases
    PRIMARY KEY (id)
);

CREATE TABLE metadata_id (
    namespace VARCHAR(255) NOT NULL,
    id BIGINT NOT NULL,
    CONSTRAINT uq_metadata_id_namespace_id UNIQUE (namespace,id)
);

CREATE TABLE metadata_aspect (
    urn VARCHAR(500) NOT NULL,
    aspect VARCHAR(200) NOT NULL,
    version BIGINT NOT NULL,
    metadata VARCHAR(500) NOT NULL,
    createdon DATETIME(6) NOT NULL,
    createdby VARCHAR(255) NOT NULL,
    createdfor VARCHAR(255),
    CONSTRAINT pk_metadata_aspect PRIMARY KEY (urn,aspect,version)
);

ALTER TABLE metadata_entity_foo ADD a_urn JSON;
ALTER TABLE metadata_entity_foo_test ADD a_urn JSON;
ALTER TABLE metadata_entity_bar ADD a_urn JSON;

-- add foo aspect to foo entity
ALTER TABLE metadata_entity_foo ADD a_aspectfoo JSON;

-- add foo aspect to foo test entity
ALTER TABLE metadata_entity_foo_test ADD a_aspectfoo JSON;

-- add bar aspect to foo entity
ALTER TABLE metadata_entity_foo ADD a_aspectbar JSON;

-- add bar aspect to foo test entity
ALTER TABLE metadata_entity_foo_test ADD a_aspectbar JSON;

-- add foobar aspect to foo entity
ALTER TABLE metadata_entity_foo ADD a_aspectfoobar JSON;

-- add foobaz aspect to foo entity
ALTER TABLE metadata_entity_foo ADD a_aspectfoobaz JSON;

-- add foobar aspect to foo entity
ALTER TABLE metadata_entity_foo ADD a_aspectfoobarbaz JSON;

-- add array aspect to foo entity
ALTER TABLE metadata_entity_foo ADD a_aspectattributes JSON;

-- add new index virtual column 'attributes'
ALTER TABLE metadata_entity_foo ADD COLUMN i_aspectattributes$attributes VARCHAR(255)
    GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(a_aspectattributes, '$.aspect.attributes')));

-- add baz aspect to foo entity
ALTER TABLE metadata_entity_foo ADD a_aspectbaz JSON;

-- add foo aspect to bar entity
ALTER TABLE metadata_entity_bar ADD a_aspectfoo JSON;

-- add baz aspect to burger entity
ALTER TABLE metadata_entity_burger ADD a_aspectfoo JSON;

-- add new index virtual column 'type'
ALTER TABLE metadata_relationship_belongstov2 ADD COLUMN `metadata$type` VARCHAR(255)
    GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(`metadata`, '$.type')));