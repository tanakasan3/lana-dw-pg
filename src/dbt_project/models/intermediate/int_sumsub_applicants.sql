select
    customer_id,
    parsed_content->>'id' as applicant_id,
    (parsed_content->>'createdAt')::timestamp as created_at,
    parsed_content->'info'->>'firstName' as first_name,
    parsed_content->'info'->>'lastName' as last_name,
    (parsed_content->'info'->>'dob')::date as date_of_birth,
    parsed_content->'info'->>'gender' as gender,
    parsed_content->'info'->>'country' as iso_alpha_3_code,
    parsed_content->'info'->>'nationality' as nationality_iso_alpha_3_code,
    parsed_content->'info'->'addresses'->0->>'formattedAddress' as formatted_address,
    -- id_documents as JSONB array (PostgreSQL doesn't support array of structs like BigQuery)
    parsed_content->'info'->'idDocs' as id_documents,
    -- questionnaires as JSONB array
    parsed_content->'questionnaires' as questionnaires

from {{ ref("stg_sumsub_applicants") }}

where parsed_content is not null and parsed_content->>'errorCode' is null
