create view gold.v_gold_takeoffs_landings as 
with 
----------------------------------------------------------------------------------------------------------------------------------
temp1 as
(select
       ae.name as nome_aeroporto
      ,ae.icao as icao_aeroporto
      ,aco.razao_social as nome_cia_aerea
      ,sum(
        case
            when vo.situacao_voo = 'REALIZADO' then 1 else 0
        end) as Decolagens
      ,0 as Pousos
      ,count(vo.icao_aerodromo_origem) as qtd_rotas
from aerodromos ae
left join vra vo
  on ae.icao = vo.icao_aerodromo_origem 
left join air_cia aco
  on vo.icao_empresa_aerea == aco.icao
where (
        case
            when vo.situacao_voo = 'REALIZADO' then 'Decolagens' else ''
        end)  != "" 
and aco.icao is not null
group by
      ae.name
      ,ae.icao 
      ,aco.razao_social
), 
----------------------------------------------------------------------------------------------------------------------------------
temp2 as 
(
  select
        ae.name as nome_aeroporto
        ,ae.icao as icao_aeroporto
        ,acd.razao_social as nome_cia_aerea
        ,0 as Decolagens
        ,sum(
          case
              when vd.situacao_voo = 'REALIZADO' then 1 else 0
          end) as Pousos
        ,count(vd.icao_aerodromo_origem) as rotas
  from aerodromos ae
  left join vra vd
    on ae.icao = vd.icao_aerodromo_destino
  left join air_cia acd
    on vd.icao_empresa_aerea == acd.icao
  where (
          case
              when vd.situacao_voo = 'REALIZADO' then 'Pousos' else ''
          end)  != "" 
  and acd.icao is not null 
group by
      ae.name
      ,ae.icao 
      ,acd.razao_social 
),
----------------------------------------------------------------------------------------------------------------------------------
temp3 as (
select * from temp1
union 
select * from temp2
)
----------------------------------------------------------------------------------------------------------------------------------
select 
   nome_aeroporto
  ,icao_aeroporto
  ,nome_cia_aerea
  ,sum(Decolagens) as qtd_decolagens
  ,sum(Pousos) as qtd_pousos
  ,sum(qtd_rotas) as qtd_rotas
from temp3
group by
  nome_aeroporto
  ,icao_aeroporto
  ,nome_cia_aerea