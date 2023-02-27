with
    pedidos as (
        select
            pedidos.id_pedido
            , pedidos.id_funcionario
            , pedidos.id_cliente
            , pedidos.id_transportadora
            , pedidos.data_do_pedido
            , pedidos.frete
            , pedidos.destinatario
            , pedidos.endereco_destinatario
            , pedidos.cep_destinatario
            , pedidos.cidade_destinatario
            , pedidos.regiao_destinatario
            , pedidos.pais_destinatario
            , pedidos.data_do_envio
            , pedidos.data_requerida                    
        from {{ ref('stg_erp__ordens')}}
    )

    , pedido_itens as (
        select
            id_pedido
            , id_produto
            , desconto
            , preco_da_unidade
            , quantidade            
        from {{ ref('stg_erp__ordem_detalhes')}}
    )

    , joined as (
        select *    
        from pedidos
        left join pedido_itens on pedidos.id_pedido = pedido_itens.id_pedido
    )

   select *
   from joined