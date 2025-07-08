-- Create the Registration_PrimaryExport table for Canadian lobbying data
CREATE TABLE "Registration_PrimaryExport" (
    reg_id_enr TEXT,
    reg_type_enr TEXT,
    effective_date_vigueur TEXT,
    end_date_fin TEXT,
    en_firm_nm_firme_an TEXT,
    client_org_corp_num TEXT,
    en_client_org_corp_nm_an TEXT,
    subsidiary_ind_filiale TEXT,
    parent_ind_soc_mere TEXT,
    rgstrnt_1st_nm_prenom_dclrnt TEXT,
    rgstrnt_last_nm_dclrnt TEXT,
    rgstrnt_address_adresse_dclrnt TEXT,
    govt_fund_ind_fin_gouv TEXT,
    fy_end_date_fin_exercice TEXT,
    posted_date_publication TEXT
);

-- Add an index on registration ID for better query performance
CREATE INDEX "idx_Registration_PrimaryExport_reg_id" ON "Registration_PrimaryExport"(reg_id_enr);

-- Add an index on posted date for date filtering
CREATE INDEX "idx_Registration_PrimaryExport_posted_date" ON "Registration_PrimaryExport"(posted_date_publication);

-- Add a comment to describe the table
COMMENT ON TABLE "Registration_PrimaryExport" IS 'Primary export table for Canadian lobbying registration data from the Commissioner of Lobbying Canada';