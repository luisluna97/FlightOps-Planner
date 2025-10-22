-- Schema de suporte para o FlightOps Planner (Fase 1)

create table if not exists voos_raw (
    event_id uuid primary key,
    flight_id uuid not null,
    temporada text not null,
    cia text not null,
    numero_voo text not null,
    act_type text not null,
    origem text not null,
    destino text not null,
    aeroporto_operacao text not null,
    evento text not null check (evento in ('ARR', 'DEP')),
    timestamp_evento timestamptz not null,
    dt_partida_utc timestamptz,
    dt_chegada_utc timestamptz,
    natureza text,
    assentos_previstos integer,
    payload jsonb,
    created_at timestamptz default now()
);

create index if not exists idx_voos_raw_aeroporto on voos_raw (aeroporto_operacao, temporada, cia);
create unique index if not exists uq_voos_raw_flight_event on voos_raw (flight_id, evento, aeroporto_operacao);

create table if not exists voos_tratados (
    voo_id uuid primary key,
    temporada text not null,
    aeroporto text not null,
    cia text not null,
    act_type text not null,
    classe_aeronave text not null,
    chegada_utc timestamptz not null,
    chegada_slot timestamptz not null,
    partida_utc timestamptz,
    partida_slot timestamptz,
    solo_min numeric,
    pnt_tst text,
    dom_int text,
    link_status text not null,
    numero_voo_in text not null,
    numero_voo_out text,
    origem text not null,
    destino text not null,
    arrival_event_id uuid not null references voos_raw(event_id),
    arrival_flight_id uuid not null,
    departure_event_id uuid references voos_raw(event_id),
    departure_flight_id uuid,
    created_at timestamptz default now()
);

create index if not exists idx_voos_tratados_lookup on voos_tratados (aeroporto, temporada, cia);
create index if not exists idx_voos_tratados_slots on voos_tratados (chegada_slot);

create table if not exists slots_atendimento (
    voo_id uuid not null references voos_tratados(voo_id) on delete cascade,
    slot_ts timestamptz not null,
    fase text not null check (fase in ('ARR', 'DEP')),
    temporada text not null,
    aeroporto text not null,
    cia text not null,
    classe_aeronave text not null,
    dom_int text,
    pnt_tst text,
    numero_voo text,
    primary key (voo_id, slot_ts, fase)
);

create index if not exists idx_slots_atendimento_main on slots_atendimento (slot_ts, aeroporto, temporada, cia);

create table if not exists slots_solo (
    voo_id uuid not null references voos_tratados(voo_id) on delete cascade,
    slot_ts timestamptz not null,
    temporada text not null,
    aeroporto text not null,
    cia text not null,
    classe_aeronave text not null,
    dom_int text,
    pnt_tst text,
    atendimento_embarque_desembarque boolean not null default false,
    atendimento_limpeza boolean not null default false,
    primary key (voo_id, slot_ts)
);

create index if not exists idx_slots_solo_main on slots_solo (slot_ts, aeroporto, temporada, cia);

alter table if exists slots_solo
    add column if not exists atendimento_embarque_desembarque boolean not null default false;

alter table if exists slots_solo
    add column if not exists atendimento_limpeza boolean not null default false;

create table if not exists param_staff_por_classe (
    classe_aeronave text primary key,
    asg integer not null default 0,
    aux_lider integer not null default 0,
    asa_rampa integer not null default 0,
    asa_triagem integer not null default 0,
    asa_desemb integer not null default 0,
    oper_equip integer not null default 0,
    updated_at timestamptz default now()
);

create table if not exists aeroportos_ref (
    codigo text primary key,
    iata text,
    icao text,
    nome text,
    cidade text,
    pais text,
    timezone text,
    tz text,
    latitude numeric,
    longitude numeric,
    altitude integer,
    updated_at timestamptz default now()
);

create unique index if not exists uq_aeroportos_ref_iata on aeroportos_ref (iata) where iata is not null;
create unique index if not exists uq_aeroportos_ref_icao on aeroportos_ref (icao) where icao is not null;

-- Visões de apoio para análises operacionais

create or replace view vw_voos_por_semana as
select
    aeroporto,
    temporada,
    date_trunc('week', chegada_slot) as semana_inicio,
    count(*) as total_voos
from voos_tratados
group by aeroporto, temporada, semana_inicio;

create or replace view vw_semana_pico_por_aeroporto as
select distinct on (aeroporto, temporada)
    aeroporto,
    temporada,
    semana_inicio,
    total_voos
from vw_voos_por_semana
order by aeroporto, temporada, total_voos desc;

create or replace view vw_dim_slot_10min as
with limites as (
    select
        min(chegada_slot) as inicio,
        max(coalesce(partida_slot, chegada_slot)) as fim
    from voos_tratados
    where chegada_slot is not null
)
select generate_series(inicio, fim, interval '10 minutes') as slot_ts
from limites
where inicio is not null and fim is not null;

create or replace view vw_dim_slot_hora as
select distinct date_trunc('hour', slot_ts) as hora
from vw_dim_slot_10min;

drop function if exists delete_airport_temporada(text, text);
drop function if exists delete_airport_temporada(text, text, integer);
drop function if exists delete_airport_temporada_step(text, text, integer);

create or replace function delete_airport_temporada_step(
    p_aeroporto text,
    p_temporada text,
    p_batch integer default 5000
)
returns text
language plpgsql
as $$
declare
    affected integer;
begin
    delete from slots_atendimento
    where ctid in (
        select ctid
        from slots_atendimento
        where aeroporto = p_aeroporto
          and (p_temporada is null or temporada = p_temporada)
        limit p_batch
    );
    GET DIAGNOSTICS affected = ROW_COUNT;
    if affected > 0 then
        return 'slots_atendimento';
    end if;

    delete from slots_solo
    where ctid in (
        select ctid
        from slots_solo
        where aeroporto = p_aeroporto
          and (p_temporada is null or temporada = p_temporada)
        limit p_batch
    );
    GET DIAGNOSTICS affected = ROW_COUNT;
    if affected > 0 then
        return 'slots_solo';
    end if;

    delete from voos_tratados
    where ctid in (
        select ctid
        from voos_tratados
        where aeroporto = p_aeroporto
          and (p_temporada is null or temporada = p_temporada)
        limit p_batch
    );
    GET DIAGNOSTICS affected = ROW_COUNT;
    if affected > 0 then
        return 'voos_tratados';
    end if;

    delete from voos_raw
    where ctid in (
        select ctid
        from voos_raw
        where aeroporto_operacao = p_aeroporto
          and (p_temporada is null or temporada = p_temporada)
        limit p_batch
    );
    GET DIAGNOSTICS affected = ROW_COUNT;
    if affected > 0 then
        return 'voos_raw';
    end if;

    return null;
end;
$$;
