select
    customer_id,
    first_name,
    last_name,
    date_of_birth,
    gender,
    countries.code as country_of_residence_code,
    countries.iso_alpha_3_code as country_of_residence_alpha_3_code,
    nationalities.code as nationality_code,
    formatted_address,
    questionnaires[0 + 1].occupation_code as occupation_code,
    questionnaires[0 + 1].economic_activity_code as economic_activity_code,
    questionnaires[0 + 1].tax_id_number as tax_id_number,
    questionnaires[0 + 1].phone_number as phone_number,
    questionnaires[0 + 1].relationship_to_bank as relationship_to_bank,
    questionnaires[0 + 1].dui as dui,
    questionnaires[0 + 1].el_salvador_municipality as el_salvador_municipality,
    questionnaires[0 + 1].marital_status as marital_status,
    questionnaires[0 + 1].married_name as married_name,
    questionnaires[0 + 1].nit as nit,
    questionnaires[0 + 1].source_of_funds as source_of_funds,
    questionnaires[0 + 1].second_nationality as second_nationality,
    id_documents[0 + 1].number as passport_number

from {{ ref("int_sumsub_applicants") }}
left join
    {{ ref("static_npb4_17_31_codigos_de_paises_o_territorios") }} as countries
    on countries.iso_alpha_3_code
    = questionnaires[0 + 1].country_of_residence_iso_alpha_3_code
left join
    {{ ref("static_npb4_17_31_codigos_de_paises_o_territorios") }} as nationalities
    on nationalities.iso_alpha_3_code = nationality_iso_alpha_3_code
