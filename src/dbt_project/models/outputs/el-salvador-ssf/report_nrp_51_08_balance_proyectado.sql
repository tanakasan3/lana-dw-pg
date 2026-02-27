select
    left({{ ident('id_codigo_cuentaproy') }}, 10) as {{ ident('id_codigo_cuentaproy') }},
    left({{ ident('nom_cuentaproy') }}, 80) as {{ ident('nom_cuentaproy') }},
    cast(round({{ ident('enero') }}, 2) as text) as {{ ident('enero') }},
    cast(round({{ ident('febrero') }}, 2) as text) as {{ ident('febrero') }},
    cast(round({{ ident('marzo') }}, 2) as text) as {{ ident('marzo') }},
    cast(round({{ ident('abril') }}, 2) as text) as {{ ident('abril') }},
    cast(round({{ ident('mayo') }}, 2) as text) as {{ ident('mayo') }},
    cast(round({{ ident('junio') }}, 2) as text) as {{ ident('junio') }},
    cast(round({{ ident('julio') }}, 2) as text) as {{ ident('julio') }},
    cast(round({{ ident('agosto') }}, 2) as text) as {{ ident('agosto') }},
    cast(round({{ ident('septiembre') }}, 2) as text) as {{ ident('septiembre') }},
    cast(round({{ ident('octubre') }}, 2) as text) as {{ ident('octubre') }},
    cast(round({{ ident('noviembre') }}, 2) as text) as {{ ident('noviembre') }},
    cast(round({{ ident('diciembre') }}, 2) as text) as {{ ident('diciembre') }}
from {{ ref("int_nrp_51_08_balance_proyectado") }}
