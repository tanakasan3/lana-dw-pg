select
    left({{ ident('id_codigo_deuda') }}, 10) as {{ ident('id_codigo_deuda') }},
    left({{ ident('desc_deuda') }}, 80) as {{ ident('desc_deuda') }},
    cast(round({{ ident('valor_deuda') }}, 2) as text) as {{ ident('valor_deuda') }}
from {{ ref("int_nrp_51_07_deuda_subordinada") }}
