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
    primary key (voo_id, slot_ts)
);

create index if not exists idx_slots_solo_main on slots_solo (slot_ts, aeroporto, temporada, cia);

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
