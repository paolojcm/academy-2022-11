with
    source_customers as (
        select 
            cast(customer_id as string) as id_cliente			
            , cast(company_name as string) as nome_da_empresa			
            , cast(contact_name as string) as nome_do_cliente		
            , cast(contact_title as string) as titulo_de_cortesia_cliente			
            , cast(address as string) as endereco_cliente			
            , cast(postal_code as string) as cep_cliente
            , cast(city as string) as cidade_cliente
            , cast(region as string) as regiao_cliente
            , cast(country as string) as pais_cliente
            , cast(phone as string) as telefone_cliente
            , cast(fax as string) as fax_cliente
        from {{ source('erp', 'customers')}}
    )

select *
from source_customers