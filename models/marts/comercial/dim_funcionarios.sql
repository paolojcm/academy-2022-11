with
    funcionarios as (
        select *            
        from {{ ref('stg_erp__funcionarios')}}
    )

    , self_joined as (
        select
            funcionarios.id_funcionario				
            , funcionarios.id_gerente	
            , funcionarios.sobrenome_do_funcionario	
            , funcionarios.nome_do_funcionario				
            , funcionarios.nome_completo_funcionario		
            , funcionarios.cargo				
            , funcionarios.titulo_de_cortesia				
            , funcionarios.data_de_nascimento				
            , funcionarios.data_da_contratacao		
            , gerentes.nome_completo_funcionario as gerente		
            , funcionarios.as endereco				
            , funcionarios.as cidade					
            , funcionarios.as regiao					
            , funcionarios.as cep	
            , funcionarios.as pais					
            , funcionarios.as telefone				
            , funcionarios.observacoes as observacoes         
        from funcionarios
        left join funcionarios as gerentes on funcionarios.id_funcionario = gerentes.id_gerente
    )

    , transformacoes as (
        select
            row_number() over (order by id_funcionario) as sk_funcionario
            , *
        from self_joined
    )

    select *
    from transformacoes