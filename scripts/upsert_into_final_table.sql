-- Upserting data from staging table into final table adzuna_jobs

MERGE INTO adzuna_jobs AS t
USING stg_adzuna_jobs AS s
ON t.job_id = s.job_id
WHEN MATCHED THEN
UPDATE
    SET job_title      = s.job_title,
        job_location   = s.job_location,
        job_company    = s.job_company,
        job_category   = s.job_category,
        job_description= s.job_description,
        job_url        = s.job_url,
        job_created    = s.job_created
WHEN NOT MATCHED THEN
INSERT (
    job_id,
    job_title,
    job_location,
    job_company,
    job_category,
    job_description,
    job_url,
    job_created
)
VALUES (
    s.job_id,
    s.job_title,
    s.job_location,
    s.job_company,
    s.job_category,
    s.job_description,
    s.job_url,
    s.job_created
);
