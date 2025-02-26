create table stg_adzuna_jobs (
    job_id BIGINT,
    job_title VARCHAR(150),
    job_location VARCHAR(150),
    job_company VARCHAR(150),
    job_category VARCHAR(150),
    job_description VARCHAR(550),
    job_url VARCHAR(150),
    job_created TIMESTAMP
);

create table adzuna_jobs (
    job_id BIGINT,
    job_title VARCHAR(150),
    job_location VARCHAR(150),
    job_company VARCHAR(150),
    job_category VARCHAR(150),
    job_description VARCHAR(550),
    job_url VARCHAR(150),
    job_created TIMESTAMP
)
