CREATE TABLE IF NOT EXISTS metadata_entity_foo (
    urn VARCHAR(100) NOT NULL,
    lastmodifiedon DATETIME(6) NOT NULL,
    lastmodifiedby VARCHAR(255) NOT NULL,
    createdfor VARCHAR(255),
    CONSTRAINT pk_metadata_aspect PRIMARY KEY (urn)
);