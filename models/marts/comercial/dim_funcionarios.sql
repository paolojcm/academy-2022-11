with
    funcionarios as (
        select *            
        from {{ ref('stg_erp__funcionarios')}}
    )

    , transformacoes as (
        select
            row_number() over (order by id_funcionario) as sk_funcionario
            , *
        from funcionarios
    )

    select *
    from transformacoes