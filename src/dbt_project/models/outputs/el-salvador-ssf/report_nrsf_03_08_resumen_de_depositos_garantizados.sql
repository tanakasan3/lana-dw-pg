select
    cast(`Correlativo` as text) as `Correlativo`,
    left(`NIU`, 25) as `NIU`,
    left(`Primer Nombre`, 30) as `Primer Nombre`,
    left(`Segundo Nombre`, 30) as `Segundo Nombre`,
    left(`Tercer Nombre`, 30) as `Tercer Nombre`,
    left(`Primer Apellido`, 30) as `Primer Apellido`,
    left(`Segundo Apellido`, 30) as `Segundo Apellido`,
    left(`Apellido de casada`, 30) as `Apellido de casada`,
    left(`Razón social`, 80) as `Razón social`,
    left(`Código del Documento`, 5) as `Código del Documento`,
    left(`Número de documento`, 25) as `Número de documento`,
    cast(round(`Total de cuentas`, 2) as text) as `Total de cuentas`,
    cast(`Saldo de capital` as text) as `Saldo de capital`,
    cast(round(`Saldo de intereses`, 2) as text) as `Saldo de intereses`,
    cast(`Saldo garantizado` as text) as `Saldo garantizado`
from {{ ref("int_nrsf_03_08_resumen_de_depositos_garantizados") }}
