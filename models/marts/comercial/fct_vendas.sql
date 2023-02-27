with
    clientes as (
        select *            
        from {{ ref('dim_clientes')}}
    )

    , funcionarios as (
        select *            
        from {{ ref('dim_funcionarios')}}
    )

    , produtos as (
        select *            
        from {{ ref('dim_produtos')}}
    )

    , transportadoras as (
        select *            
        from {{ ref('dim_transportadoras')}}
    )

    , pedido_item as (
        select *            
        from {{ ref('int_vendas__pedido_itens')}}
    )

    , joined as (
        select
            pedido_item.id_pedido
            , pedido_item.id_funcionario
            , pedido_item.id_cliente
            , pedido_item.id_transportadora            
            , pedido_item.id_produto           
            , pedido_item.desconto
            , pedido_item.preco_da_unidade
            , pedido_item.quantidade             
            , pedido_item.frete
            , pedido_item.data_do_pedido
            , pedido_item.data_do_envio
            , pedido_item.data_requerida  
            , pedido_item.destinatario
            , pedido_item.endereco_destinatario
            , pedido_item.cep_destinatario
            , pedido_item.cidade_destinatario
            , pedido_item.regiao_destinatario
            , pedido_item.pais_destinatario 
        from pedido_item
        left join clientes on pedido_item.id_cliente = clientes.id_cliente
        left join funcionarios on pedido_item.id_funcionario = funcionarios.id_funcionario
        left join produtos on pedido_item.id_produto = produtos.id_produto
        left join transportadoras on pedido_item.id_transportadora = transportadoras.id_transportadora
    )

    select *
    from joined
    