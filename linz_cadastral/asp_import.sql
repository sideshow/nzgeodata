-- This script imports ASP street data ONLY. Does not import any non-road data.
-- Be sure to update the COPY statement with the correct path to the ASP street.txt file.

--drop table asp_street;

create table asp_street (
    id integer NOT NULL PRIMARY KEY,
    sufi bigint NOT NULL,
    name character varying NOT NULL,
    locality character varying NOT NULL,
    created character varying NULL,
    modified character varying NULL,
    text character varying NULL,
    unofficial_flag character NULL,
    editor character varying NULL,
    version integer NULL,
    edit_action character NULL,
    status character NULL
);

COPY asp_street FROM 'landonline/asp/street.txt' DELIMITERS '|' CSV HEADER;

-- (delete historic records)
DELETE FROM asp_street where status = 'H';

CREATE INDEX idx_asp_street_sufi on asp_street (sufi);

VACUUM ANALYZE asp_street;