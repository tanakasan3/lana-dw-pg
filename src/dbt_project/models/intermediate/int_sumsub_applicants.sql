with base as (
    select
        customer_id,
        parsed_content,
        parsed_content->>'id' as applicant_id,
        (parsed_content->>'createdAt')::timestamp as created_at,
        parsed_content->'info'->>'firstName' as first_name,
        parsed_content->'info'->>'lastName' as last_name,
        (parsed_content->'info'->>'dob')::date as date_of_birth,
        parsed_content->'info'->>'gender' as gender,
        parsed_content->'info'->>'country' as iso_alpha_3_code,
        parsed_content->'info'->>'nationality' as nationality_iso_alpha_3_code,
        parsed_content->'info'->'addresses'->0->>'formattedAddress' as formatted_address
    from {{ ref("stg_sumsub_applicants") }}
    where parsed_content is not null and parsed_content->>'errorCode' is null
),

-- Extract id_documents array with flattened structure
id_docs as (
    select
        customer_id,
        jsonb_agg(
            jsonb_build_object(
                'iso_alpha_3_code', doc->>'country',
                'document_type', doc->>'idDocType',
                'number', doc->>'number'
            )
        ) as id_documents
    from base
    cross join lateral jsonb_array_elements(parsed_content->'info'->'idDocs') as doc
    group by customer_id
),

-- Extract questionnaires array with flattened structure (deeply nested paths)
questionnaire_data as (
    select
        customer_id,
        jsonb_agg(
            jsonb_build_object(
                'occupation_code', q->'sections'->'personalInformation'->'items'->'occupation'->>'value',
                'nit', q->'sections'->'personalInformation'->'items'->'nit'->>'value',
                'source_of_funds', q->'sections'->'personalInformation'->'items'->'sourceOfFunds'->>'value',
                'second_nationality', q->'sections'->'personalInformation'->'items'->'secondNationality'->>'value',
                'marital_status', q->'sections'->'personalInformation'->'items'->'maritalStatus'->>'value',
                'married_name', q->'sections'->'personalInformation'->'items'->'marriedName'->>'value',
                'economic_activity_code', q->'sections'->'personalInformation'->'items'->'economicActivity'->>'value',
                'country_of_residence_iso_alpha_3_code', q->'sections'->'personalInformation'->'items'->'countryOfResidence'->>'value',
                'tax_id_number', q->'sections'->'personalInformation'->'items'->'taxIdentificationNum'->>'value',
                'phone_number', q->'sections'->'personalInformation'->'items'->'phoneNumber'->>'value',
                'relationship_to_bank', q->'sections'->'personalInformation'->'items'->'relationshipToBank'->>'value',
                'dui', q->'sections'->'personalInformation'->'items'->'DUI'->>'value',
                'el_salvador_municipality', q->'sections'->'personalInformation'->'items'->'municipality'->>'value'
            )
        ) as questionnaires
    from base
    cross join lateral jsonb_array_elements(parsed_content->'questionnaires') as q
    group by customer_id
)

select
    base.customer_id,
    base.applicant_id,
    base.created_at,
    base.first_name,
    base.last_name,
    base.date_of_birth,
    base.gender,
    base.iso_alpha_3_code,
    base.nationality_iso_alpha_3_code,
    base.formatted_address,
    coalesce(id_docs.id_documents, '[]'::jsonb) as id_documents,
    coalesce(questionnaire_data.questionnaires, '[]'::jsonb) as questionnaires
from base
left join id_docs using (customer_id)
left join questionnaire_data using (customer_id)
