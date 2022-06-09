DROP TABLE IF EXISTS metadata_entity_foo;

-- initialize foo entity table
CREATE TABLE metadata_entity_foo (
                                     urn VARCHAR(100) NOT NULL,
                                     lastmodifiedon DATETIME(6) NOT NULL,
                                     lastmodifiedby VARCHAR(255) NOT NULL,
                                     CONSTRAINT pk_metadata_aspect PRIMARY KEY (urn)
);

-- add foo aspect to foo entity
ALTER TABLE metadata_entity_foo ADD a_testing_aspectfoo LONGTEXT;

-- add new index virtual column 'value'
ALTER TABLE metadata_entity_foo ADD COLUMN i_testing_aspectfoo$value VARCHAR(255)
    GENERATED ALWAYS AS (a_testing_aspectfoo ->> '$.value') VIRTUAL;

-- create index for index column
CREATE INDEX i_testing_aspectfoo$value ON metadata_entity_foo (urn, i_testing_aspectfoo$value);