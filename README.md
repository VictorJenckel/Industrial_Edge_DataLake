# Industrial Data Gateway — Linhas de Inspeção de Vidro Plano

> **O gateway de dados seguro e escalável para o monitoramento industrial.**
>
> Dos logs brutos das máquinas a uma camada de dados estruturada, auditada e sempre ativa — pronta para alimentar servidores externos e *dashboards* de produção.

---

## O que é este projeto

Este projeto é a **espinha dorsal da infraestrutura de dados**, atuando como um *edge gateway* para linhas de inspeção de espelhos e vidros planos operando em uma fábrica real no Brasil.

Ele ingere, desduplica e armazena eventos de inspeção das máquinas **Eagle Vision** — linhas `Mirror1`, `Cut1` e `Cut2` — em um banco de dados **PostgreSQL particionado**, orquestrado pelo **Apache Airflow** e conteinerizado via **Docker**.

Os ativos físicos monitorados são máquinas de inspeção óptica automatizada que escaneiam chapas de vidro em busca de defeitos em velocidade de produção.

Sua saída bruta — logs de eventos com *timestamps* — é tratada e centralizada para que servidores externos possam construir *dashboards* de alto nível com dados absolutos e confiáveis.

> **Este não é um tutorial ou um ambiente de testes (*sandbox*).**
>
> O sistema roda em produção, 24 horas por dia, lidando com dados reais de manufatura.

---

## Por que esta arquitetura existe

Máquinas industriais não se comportam como APIs web.

Elas travam, perdem energia, produzem registros duplicados, estouram contadores e, às vezes, registram três eventos com exatamente o mesmo *timestamp* em milissegundos.

Cada decisão de engenharia neste projeto existe para lidar com essas realidades antes de entregar os dados para o nível gerencial.

### Desduplicação determinística

Os arquivos de log são relidos continuamente.

Em vez de depender de *timestamps* — que não são determinísticos na velocidade da máquina — o *pipeline* gera uma chave sequencial determinística (`event_seq`) baseada na posição física de leitura via `cumcount()`.

O comando `ON CONFLICT DO NOTHING` do PostgreSQL torna cada inserção **idempotente**.

Isso permite que o *pipeline* seja executado continuamente sem criar registros duplicados.

### Resiliência a casos extremos (*Edge Cases*)

O *pipeline* lida explicitamente com situações como:

- Eventos simultâneos — 3 a 4 chapas de vidro escaneadas no mesmo milissegundo.
- Falhas de leitura de sensores.
- IDs falsos com valor `0`.
- Estouro de contadores (*rollover*) de 12 bits do CLP no limite do hardware.
- Reinicializações e interrupções dos equipamentos.
- Reprocessamento dos mesmos arquivos de origem.

### Auditoria contínua de integridade

Uma DAG dedicada do **Apache Airflow** roda diariamente para reconciliar matematicamente os arquivos físicos (`.csv` / `.txt`) com o banco de dados.

Qualquer discrepância aciona um alerta.

Dessa forma, a perda ou inconsistência de dados é detectada antes que o problema se acumule.

### Arquivamento frio sem tempo de inatividade (*Zero Downtime*)

A cada trimestre, um binário desenvolvido em **Rust** extrai dados históricos do PostgreSQL, comprime-os para **Parquet com Snappy** e executa um `DROP TABLE` instantâneo nas partições fechadas.

O processo libera espaço em disco sem bloquear as tabelas de produção.

---

## Arquitetura

```text
┌─────────────────────────────────────────────────────────────┐
│                  CAMADA FÍSICA — CHÃO DE FÁBRICA            │
│                                                             │
│  Eagle Vision Mirror1 ─┐                                    │
│  Eagle Vision Cut1    ─┼── Logs de inspeção (.txt / .csv)   │
│  Eagle Vision Cut2    ─┘                                    │
└───────────────────────────────┬─────────────────────────────┘
                                │
                                │ SMBClient
                                │ (Windows XP legado)
                                │
                                │ rsync
                                │ (Linux moderno)
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                     ÁREA DE STAGING                         │
│                       Servidor Local                         │
└───────────────────────────────┬─────────────────────────────┘
                                │
                                │ Airflow DAG
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                        RUST ETL                              │
│                                                             │
│  ├── Coerção de tipos                                       │
│  ├── Validação de regras de negócio                         │
│  ├── Geração determinística de event_seq                    │
│  └── Inserção idempotente no PostgreSQL                     │
│      ON CONFLICT DO NOTHING                                 │
└───────────────────────────────┬─────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                    POSTGRESQL                               │
│                  Banco particionado                         │
│                                                             │
│  ├── Dados brutos e tratados                                │
│  │   └── Uma partição por dia e por linha                   │
│  │                                                           │
│  ├── DAG de Auditoria                                       │
│  │   └── Reconciliação diária às 23:30                      │
│  │                                                           │
│  └── Arquivamento frio                                      │
│      └── Trimestral → Parquet + Snappy                      │
└───────────────────────────────┬─────────────────────────────┘
                                │
                                │ Disponibilização de Dados
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                    SERVIDOR EXTERNO                         │
│                                                             │
│  Dashboards • Relatórios • Analytics                        │
└─────────────────────────────────────────────────────────────┘
## A camada PostgreSQL atua como um repositório central altamente confiável.

Uma vez tratados e persistidos, os dados ficam prontamente disponíveis para consultas por sistemas externos, garantindo que os dashboards de produção reflitam o estado real da fábrica sem sobrecarregar a rede de automação ou as próprias máquinas.

## Stack de Tecnologia

| Camada | Tecnologia | Papel |
|---|---|---|
| SO & Contêineres | Linux + Docker Compose | Hospedagem e isolamento dos serviços |
| Banco de Dados | PostgreSQL | Armazenamento particionado e disponibilização de dados |
| Orquestração | Apache Airflow | Agendamento de DAGs e gerenciamento do pipeline |
| Processamento ETL | Rust + Python | Rust para ingestão e processamento; Python para lógica das DAGs |
| Monitoramento | Prometheus + Grafana | Observabilidade da infraestrutura e do pipeline |
| Transporte de Arquivos | SMBClient + rsync | Coleta de logs de máquinas modernas e legadas |
| Backup | Crontab + SMBClient | Recuperação de desastres automatizada para pasta de rede |
| Armazenamento Frio | Parquet + Snappy | Arquivamento trimestral e liberação de espaço em produção |
Rede e Segurança

O servidor opera em uma rede de controle industrial restrita (Camada P2), isolada da rede corporativa de TI.

O acesso aos dados pelo servidor de dashboards é controlado através de um gateway OT/IT preexistente.

O firewall (ufw) permite apenas as portas necessárias:

8081/tcp  — Interface Web do Airflow
5432/tcp  — PostgreSQL
3000/tcp  — Grafana
22/tcp    — Administração remota via SSH

A arquitetura mantém o processamento e armazenamento dos dados próximos à origem, reduzindo a dependência da rede corporativa e evitando tráfego desnecessário entre OT e IT.

Estrutura do Projeto
datalake_local/
│
├── dags/
│   ├── ingestion_mr1.py
│   ├── ingestion_cut1.py
│   ├── ingestion_cut2.py
│   ├── audit_daily.py
│   └── archive_quarterly.py
│
├── rust_etl/
│   ├── src/
│   └── Cargo.toml
│
├── sql/
│   ├── schema/
│   ├── partitions/
│   └── audit/
│
├── monitoring/
│   ├── prometheus/
│   └── grafana/
│
├── docker-compose.yml
└── README.md
Como Executar

O sistema foi projetado para operar de forma 100% autônoma depois de implantado.

1. Clonar o repositório
git clone https://github.com/VictorJenckel/local_cluster_industrial_data
cd local_cluster_industrial_data
2. Configurar as variáveis de ambiente
cp .env.example .env

Configure no arquivo .env:

Diretórios de staging.
Credenciais do PostgreSQL.
Endereços das máquinas SMB.
Caminhos dos arquivos de log.
Destinos de backup.
Configurações de rede.

3. Iniciar os serviços
docker compose up -d

4. Compilar os binários ETL em Rust
cd rust_etl
cargo build --release
Interfaces de Gerenciamento

As interfaces podem ser acessadas a partir de máquinas autorizadas na rede industrial utilizando o IP estático do servidor.

Serviço	URL
Airflow	http://<ip-do-servidor>:8081
Grafana	http://<ip-do-servidor>:3000

Requisitos
Linux — Ubuntu 22.04+
Docker
Docker Compose Plugin
Rust Toolchain
SMBClient
PostgreSQL
Apache Airflow
Características Técnicas
Ingestão
Processamento contínuo de arquivos de inspeção.
Suporte a máquinas legadas e modernas.
Coleta via SMB e rsync.
Processamento incremental.
Reprocessamento seguro.
Integridade
Chave determinística event_seq.
Inserções idempotentes.
ON CONFLICT DO NOTHING.
Auditoria diária.
Reconciliação entre arquivos físicos e banco de dados.
Detecção de inconsistências.
Escalabilidade
PostgreSQL particionado por data.
Separação entre ingestão e consumo.
Arquivamento histórico em Parquet.
Processamento próximo à origem dos dados.
Redução do tráfego na rede OT.
Observabilidade
Apache Airflow para acompanhamento dos pipelines.
Prometheus para métricas.
Grafana para visualização.
DAG dedicada para auditoria de integridade.
Dados e Fluxo de Informação

O fluxo completo pode ser resumido da seguinte forma:

Máquina de Inspeção
        │
        ▼
Logs Brutos
        │
        ▼
Staging
        │
        ▼
Rust ETL
        │
        ├── Validação
        ├── Normalização
        ├── Deduplicação
        └── Geração de event_seq
        │
        ▼
PostgreSQL
        │
        ├── Consultas operacionais
        ├── Auditoria
        └── Arquivamento
        │
        ▼
Servidor Externo
        │
        ├── Dashboards
        ├── Relatórios
        └── Analytics
Princípios de Engenharia

Este projeto foi desenvolvido seguindo alguns princípios fundamentais:

Os dados devem ser confiáveis antes de chegar ao nível gerencial.
A ingestão deve ser idempotente.
Falhas de máquinas não podem causar perda silenciosa de dados.
A auditoria deve ser automatizada.
A infraestrutura OT deve permanecer isolada.
O processamento deve ocorrer próximo à origem sempre que possível.
Dados históricos devem ser removidos da camada operacional sem perda de informação.
A arquitetura deve continuar funcionando mesmo quando os equipamentos apresentarem comportamento inesperado.
Status do Projeto

Production / Industrial Environment

O sistema foi desenvolvido para operação em ambiente industrial real, processando dados provenientes de linhas de inspeção óptica de vidro plano.

Principais capacidades
 Ingestão automática
 Processamento incremental
 Deduplicação determinística
 Inserção idempotente
 PostgreSQL particionado
 Orquestração com Airflow
 Auditoria diária
 Monitoramento com Prometheus
 Dashboards com Grafana
 Backup automatizado
 Arquivamento histórico em Parquet
 Operação contínua em ambiente industrial
Sobre o Autor

Criado e mantido por Victor Jenckel.

Engenheiro de automação industrial com mais de 15 anos de experiência em chão de fábrica, especializado em automação industrial, integração OT/IT e engenharia de dados industriais.

O projeto representa a aplicação prática de conceitos de Data Engineering, Industrial IoT, OT/IT Integration e Edge Computing em um ambiente de manufatura real.

🇧🇷 Taubaté, SP — Brasil

📧 victorjenckel@gmail.com

💼 linkedin.com/in/victorjenckel
