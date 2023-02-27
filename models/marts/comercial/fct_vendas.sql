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
            , funcionarios.sk_funcionario as fk_funcionario
            , clientes.sk_cliente as fk_cliente
            , transportadoras.sk_transportadora as fk_transportadora
            , produtos.sk_produto as fk_produto           
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
            , clientes.nome_do_cliente
            , funcionarios.nome_completo_funcionario
            , funcionarios.id_gerente
            , transportadoras.nome_transportadora
            , produtos.nome_produto
            , produtos.nome_categoria
            , produtos.is_discontinuado
        from pedido_item
        left join clientes on pedido_item.id_cliente = clientes.id_cliente
        left join funcionarios on pedido_item.id_funcionario = funcionarios.id_funcionario
        left join produtos on pedido_item.id_produto = produtos.id_produto
        left join transportadoras on pedido_item.id_transportadora = transportadoras.id_transportadora
    )

    , transformacoes as (
        select
            --id_pedido || '-' || fk_produto as sk_venda   
            {{ dbt_utils.surrogate_key(['id_pedido', 'fk_produto'])}} as sk_venda
            , *
            , case
                when desconto > 0 then true
                else false
                end as is_teve_desconto
            , preco_da_unidade * quantidade as total_bruto
            , (1 - desconto) * preco_da_unidade * quantidade as total_liquido
        from joined
    )

    select *
    from transformacoes
    