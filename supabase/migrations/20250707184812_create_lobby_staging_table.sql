-- Create the lobby_staging table for Canadian lobbying data
CREATE TABLE lobby_staging (
    reg_id_enr TEXT,
    bnf_type TEXT,
    en_bnf_nm_an TEXT,
    fr_bnf_nm TEXT,
    street_rue_1 TEXT,
    street_rue_2 TEXT,
    city_ville TEXT,
    post_code_postal TEXT,
    prov_state_prov_etat TEXT,
    country_pays TEXT
);

-- Add an index on registration ID for better query performance
CREATE INDEX idx_lobby_staging_reg_id ON lobby_staging(reg_id_enr);

-- Add an index on country for filtering
CREATE INDEX idx_lobby_staging_country ON lobby_staging(country_pays);

-- Add a comment to describe the table
COMMENT ON TABLE lobby_staging IS 'Staging table for Canadian lobbying registration data from the Commissioner of Lobbying Canada';