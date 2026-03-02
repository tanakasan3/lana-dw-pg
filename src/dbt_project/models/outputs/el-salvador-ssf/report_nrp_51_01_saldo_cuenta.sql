select
    to_char(round({{ ident('valor') }}, 2), 'FM9999999990.00') as {{ ident('valor') }},
    right({{ ident('id_codigo_cuenta') }}, 10) as {{ ident('id_codigo_cuenta') }},
    upper(left(regexp_replace({{ ident('nom_cuenta') }}, '[&<>"]', '_', 'g'), 80)) as {{ ident('nom_cuenta') }}
from {{ ref("int_nrp_51_01_saldo_cuenta") }}
