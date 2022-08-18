DROP TABLE IF EXISTS metadata_entity_foo;
DROP TABLE IF EXISTS metadata_entity_bar;
DROP TABLE IF EXISTS metadata_aspect;
DROP TABLE IF EXISTS metadata_id;
DROP TABLE IF EXISTS metadata_index;


-- initialize foo entity table
CREATE TABLE IF NOT EXISTS metadata_entity_foo (
    urn VARCHAR(100) NOT NULL,
    lastmodifiedon DATETIME(6) NOT NULL,
    lastmodifiedby VARCHAR(255) NOT NULL,
    createdfor VARCHAR(255),
    CONSTRAINT pk_metadata_aspect PRIMARY KEY (urn)
);

-- initialize bar entity table
CREATE TABLE IF NOT EXISTS metadata_entity_bar (
    urn VARCHAR(100) NOT NULL,
    lastmodifiedon DATETIME(6) NOT NULL,
    lastmodifiedby VARCHAR(255) NOT NULL,
    createdfor VARCHAR(255),
    CONSTRAINT pk_metadata_aspect PRIMARY KEY (urn)
);

CREATE TABLE metadata_id (
                             namespace                     VARCHAR(255) NOT NULL,
                             id                            BIGINT NOT NULL,
                             CONSTRAINT uq_metadata_id_namespace_id UNIQUE (namespace,id)
);

CREATE TABLE metadata_aspect (
                                 urn                           VARCHAR(500) NOT NULL,
                                 aspect                        VARCHAR(200) NOT NULL,
                                 version                       BIGINT NOT NULL,
                                 metadata                      VARCHAR(500) NOT NULL,
                                 createdon                     DATETIME(6) NOT NULL,
                                 createdby                     VARCHAR(255) NOT NULL,
                                 createdfor                    VARCHAR(255),
                                 CONSTRAINT pk_metadata_aspect_ PRIMARY KEY (urn,aspect,version)
);

CREATE TABLE metadata_index (
                                id                            BIGINT AUTO_INCREMENT NOT NULL,
                                urn                           VARCHAR(500) NOT NULL,
                                aspect                        VARCHAR(200) NOT NULL,
                                path                          VARCHAR(200) NOT NULL,
                                longval                       BIGINT,
                                stringval                     VARCHAR(500),
                                doubleval                     DOUBLE,
                                CONSTRAINT pk_metadata_index PRIMARY KEY (id)
);

-- add foo aspect to foo entity
ALTER TABLE metadata_entity_foo ADD a_aspectfoo LONGTEXT;

-- add new index virtual column 'value'
ALTER TABLE metadata_entity_foo ADD COLUMN i_aspectfoo$value VARCHAR(255)
    GENERATED ALWAYS AS (a_aspectfoo ->> '$.aspect.value') VIRTUAL;

-- add bar aspect to foo entity
ALTER TABLE metadata_entity_foo ADD a_aspectbar LONGTEXT;

-- add new index virtual column 'value'
ALTER TABLE metadata_entity_foo ADD COLUMN i_aspectbar$value VARCHAR(255)
    GENERATED ALWAYS AS (a_aspectbar ->> '$.aspect.value') VIRTUAL;

-- create index for index column
CREATE INDEX i_aspectfoo$value ON metadata_entity_foo (urn(50), i_aspectfoo$value);

-- create index for index column
CREATE INDEX i_aspectbar$value ON metadata_entity_foo (urn(50), i_aspectbar$value);


-- create index idx_long_val on metadata_index (aspect,path(50),longval,urn(50));
-- create index idx_string_val on metadata_index (aspect,path(50),stringval,urn(50));
-- create index idx_double_val on metadata_index (aspect,path(50),doubleval,urn(50));
-- create index idx_urn on metadata_index (urn(50));