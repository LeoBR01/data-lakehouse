# 🏗️ Modern Data Lakehouse

Pipeline analítico moderno com dbt, DuckDB e Apache Airflow processando 
eventos reais do GitHub Archive.

## Arquitetura
```
GitHub Archive (180k eventos/hora)
        ↓
   scripts/load_raw.py
   (ingestão para DuckDB)
        ↓
   dbt Models
   ├── staging      → limpeza e padronização
   ├── intermediate → lógica de negócio
   └── marts        → tabelas Gold para analytics
        ↓
   Airflow DAG
   (orquestração completa)
```

## Stack
- **DuckDB** — banco analítico embarcado, zero configuração
- **dbt** — transformações SQL versionadas, testadas e documentadas
- **Apache Airflow** — orquestração de pipelines como código
- **GitHub Archive** — fonte de dados pública com eventos reais do GitHub

## Decisões de Arquitetura
- **Medallion Architecture** — Bronze (staging) → Silver (intermediate) → Gold (marts)
- **dbt materializations** — views nas camadas Bronze/Silver, tabela física no Gold
- **Deduplicação com QUALIFY + ROW_NUMBER** — remove duplicatas da fonte
- **Filtro de bots** — exclui atores com padrão de bot para dados mais confiáveis
- **DAG sem schedule** — execução manual para controle explícito do pipeline

## Estrutura do Projeto
```
dags/          → DAGs do Airflow
dbt/
├── models/
│   ├── staging/      → Bronze: dados brutos limpos
│   ├── intermediate/ → Silver: lógica de negócio
│   └── marts/        → Gold: tabelas finais analíticas
├── tests/            → testes customizados
└── seeds/            → dados estáticos
scripts/       → scripts de ingestão
data/          → dados brutos (ignorado no Git)
```

## Como rodar

**Instalar dependências:**
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

**Carregar dados:**
```bash
python scripts/load_raw.py
```

**Rodar pipeline dbt:**
```bash
cd dbt
dbt run
dbt test
```

**Ver documentação e lineage:**
```bash
dbt docs generate && dbt docs serve
```

**Iniciar Airflow:**
```bash
export AIRFLOW_HOME=~/projetos/data-lakehouse/airflow
airflow standalone
```

**Acessar Airflow UI:** http://localhost:8080

## Status do Projeto
- [x] GitHub Archive — 180k eventos reais ingeridos
- [x] DuckDB — armazenamento analítico local
- [x] dbt staging — limpeza e padronização dos dados
- [x] dbt intermediate — lógica de negócio aplicada
- [x] dbt marts — tabela Gold para analytics
- [x] dbt test — qualidade de dados automática
- [x] dbt docs — lineage visual gerado automaticamente
- [x] Airflow DAG — orquestração completa do pipeline