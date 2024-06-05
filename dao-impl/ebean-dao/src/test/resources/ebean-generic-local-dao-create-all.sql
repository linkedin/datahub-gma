DROP TABLE IF EXISTS metadata_aspect;

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