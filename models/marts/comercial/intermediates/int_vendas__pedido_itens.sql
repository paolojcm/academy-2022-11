with
    pedidos as (
        select
            id_pedido
            , id_funcionario
            , id_cliente
            , id_transportadora
            , data_do_pedido
            , frete
            , destinatario
            , endereco_destinatario
            , cep_destinatario
            , cidade_destinatario
            , regiao_destinatario
            , pais_destinatario
            , data_do_envio
            , data_requerida                    
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
        select 
            pedidos.id_pedido
            , pedidos.id_funcionario
            , pedidos.id_cliente
            , pedidos.id_transportadora            
            , pedido_itens.id_produto           
            , pedido_itens.desconto
            , pedido_itens.preco_da_unidade
            , pedido_itens.quantidade             
            , pedidos.frete
            , pedidos.data_do_pedido
            , pedidos.data_do_envio
            , pedidos.data_requerida  
            , pedidos.destinatario
            , pedidos.endereco_destinatario
            , pedidos.cep_destinatario
            , pedidos.cidade_destinatario
            , pedidos.regiao_destinatario
            , pedidos.pais_destinatario
         
        from pedidos
        left join pedido_itens on pedidos.id_pedido = pedido_itens.id_pedido
    )

   select *
   from joined