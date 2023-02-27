with
    pedidos as (
        select *            
        from {{ ref('dim_clientes')}}
    )

    , pedido_itens as (
        select *            
        from {{ ref('dim_funcionarios')}}
    )