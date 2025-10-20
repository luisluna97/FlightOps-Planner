# FlightOps Planner ETL

Back-end tooling para **Fase 1** do FlightOps Planner: extrair a malha do SIROS, tratar e casar voos com base em regras operacionais, e publicar os dados normalizados no Supabase.

## Estrutura

```
FlightOps Planner/
├─ src/
│  └─ flightops_planner/
│     ├─ __init__.py
│     ├─ config.py
│     ├─ logging_utils.py
│     └─ supabase_client.py
├─ requirements.txt
└─ README.md
```

Os módulos adicionais (ingestão, parser, linking, expansão de slots e carregamento) serão adicionados ao longo desta fase.

## Dependências

- Python 3.11+
- Bibliotecas listadas em `requirements.txt`

Instalação sugerida:

```bash
python -m venv .venv
source .venv/bin/activate  # (Linux/Mac)
.venv\Scripts\Activate.ps1 # (Windows)
pip install -r requirements.txt
```

## Configuração (.env)

O pipeline lê as variáveis via `config.py` (Pydantic). Crie um arquivo `.env` na raiz com, no mínimo:

```
SUPABASE_URL="https://<sua-instancia>.supabase.co"
SUPABASE_SERVICE_ROLE_KEY="<service-role-key>"
SUPABASE_SCHEMA="public"
SUPABASE_VERIFY_SSL=true         # defina como false se sua rede interceptar SSL
DEFAULT_SEASON="S25"            # opcional, usado como fallback
HTTP_TIMEOUT_SECONDS=30
HTTP_CONCURRENCY=4
SIROS_BASE_URL="https://sas.anac.gov.br/sas/siros_api"
SIROS_VERIFY_SSL=true
MIN_TURNAROUND_MINUTES=30        # casamento exige pelo menos 30 minutos de solo
SOLO_OPEN_MINUTES=180            # solo “aberto” quando não há partida linkada
ROUNDING_GRANULARITY_MINUTES=10  # slots de 10 minutos conforme especificação
```

> ⚠️ Use a **service role key** apenas em ambientes de backend/segurança controlada. Nunca exponha esse valor no front-end.

Carregue a `.env` antes de executar o pipeline (por exemplo com `python-dotenv`) ou exporte as variáveis diretamente no ambiente.

## Carga de aeroportos (referência)

O SIROS utiliza códigos ICAO, mas a interface costuma trabalhar com IATA. Para cruzar as informações, carregue o CSV `airports.csv` (OurAirports) para o Supabase.

```bash
python -m flightops_planner.reference_loader airports.csv
```

O loader cria/atualiza a tabela `aeroportos_ref`, usando IATA como chave prioritária (fall back para ICAO). Essa tabela é consultada pelo ETL para classificar voos DOM/INT e traduzir ICAO⇄IATA.

## Próximos Passos

1. Implementar cliente SIROS + parsing da malha.
2. Criar lógica de casamento com `>= 30min` de tempo de solo e arredondamento para slots de 10min.
3. Persistir (`upsert`) em tabelas Supabase (`voos_raw`, `voos_tratados`, `slots_atendimento`, `slots_solo`, `param_staff_por_classe`).

A interface final do front pode ser construída no v0.dev ou neste projeto. Como o pipeline ficará exposto via Supabase, qualquer front-end que consuma os endpoints / views será compatível. No momento o foco está em concluir o backend/ETL.

## Execução do ETL

Após configurar o ambiente e preencher o `.env`, rode:

```bash
python run_phase1.py --airport GRU --airport GIG --season S25
```

Parâmetros úteis:

- `--airport`: aceita múltiplas ocorrências (GRU, GIG, SSA...).
- `--season`: temporada SIROS (fallback para `DEFAULT_SEASON` se omitido).
- `--airports-csv`: caminho para `airports.csv`; opcionalmente atualiza `aeroportos_ref` antes do ETL.
- `--dry-run`: processa tudo sem enviar para o Supabase.
- `--log-level=DEBUG`: detalhamento adicional.

O ETL automaticamente carrega a tabela `aeroportos_ref` no Supabase para inferir país de cada aeroporto, permitindo classificar voos como DOM/INT.

## Estrutura de Banco

Em `sql/001_create_tables.sql` há o script idempotente para criar as tabelas:

- `voos_raw`
- `voos_tratados`
- `slots_atendimento`
- `slots_solo`
- `param_staff_por_classe`
- `aeroportos_ref`

Os índices sugeridos seguem as premissas da fase 1 (filtros por aeroporto/mês/cia e grids 7x144). Execute o script diretamente no Supabase antes da primeira carga.
