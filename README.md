# Projeto Data Server Local (On-Premises)

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/postgresql-4169e1?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)


Este projeto implementa um **mini Data Lake local** rodando em um servidor Linux, com banco de dados **PostgreSQL** e orquestração de processos via **Apache Airflow**.  
O objetivo é criar uma infraestrutura robusta e escalável para integrar e armazenar dados industriais, aplicando boas práticas de engenharia de dados para lidar com os desafios reais do chão de fábrica.

---

## 🏭 Motivação

Este projeto foi criado como parte da minha jornada de transição da área de automação industrial para a engenharia de dados.
Com mais de 15 anos de experiência em chão de fábrica e sistemas industriais (especialmente CLPs Siemens e integração de redes), minha missão é unir esse conhecimento operacional (OT) com pipelines de dados, ETL e Machine Learning (IT), começando por uma base sólida de ingestão, orquestração e confiabilidade de dados.

---

## 🛠️ Stack Utilizada

| Camada               | Ferramenta        | Função                                                                 |
|----------------------|-------------------|------------------------------------------------------------------------|
| **SO** | Linux (Ubuntu)    | Ambiente principal de hospedagem.                                      |
| **Banco de Dados** | PostgreSQL        | Armazenamento analítico com particionamento por data.                  |
| **Orquestração** | Apache Airflow    | Agendamento, controle de dependências e execução das pipelines ETL.    |
| **Processamento** | Python (Pandas)   | Limpeza, deduplicação em memória e transformações determinísticas.     |
| **Monitoramento** | Prometheus + Grafana| Observabilidade da infraestrutura e dos pipelines.                     |
| **Automação (OS)** | Systemd / Crontab | Gerenciamento de serviços e rotinas de backup.                         |

---

## 🧠 Arquitetura e Soluções de Engenharia

Para garantir que a **Camada Raw (Bronze)** seja um espelho perfeito da máquina sem perda de dados, este projeto implementa soluções avançadas para anomalias industriais:

* **Idempotência e Chaves Determinísticas:** Arquivos de log são frequentemente relidos pelas DAGs. Para evitar duplicação, o pipeline gera um sequencial determinístico (`event_seq` via Pandas `cumcount`) baseado na posição física da leitura. Inserções usam `ON CONFLICT DO NOTHING` no PostgreSQL, garantindo que o ETL possa rodar infinitas vezes sem gerar registros duplicados.
* **Resiliência a Edge Cases Físicos:** O pipeline trata eventos simultâneos (ex: 3 a 4 chapas lidas no mesmo milissegundo pelo scanner), falhas de leitura de sensores (IDs espúrios ou zerados) e o limite de *rollover* de contadores de CLPs de 12 bits, preservando o histórico exato da produção.
* **Auditoria Contínua:** Uma DAG dedicada roda diariamente para reconciliar matematicamente os arquivos físicos (`.csv` / `.txt`) contra o banco de dados, garantindo 100% de integridade e alertando sobre qualquer *data loss*.

---

## 📂 Estrutura do Projeto

`datalake_local/`

![ArqDBC_airflow](https://github.com/user-attachments/assets/40c52b16-ab68-4e3f-bbf0-dbcaadb4347a)

---

## ⚙️ Funcionamento

### 1. Automações de Sistema
- Foram criados serviços usando o `systemd` para iniciar o Airflow Webserver e o Scheduler automaticamente no boot da máquina.

### 2. ETL das Máquinas (Ingestão)
- **Máquinas Legadas (Windows XP):** Geram arquivos de log `.txt`. Os arquivos são copiados para uma pasta local temporária e sobrescritos a cada 5 minutos usando `smbclient` orquestrado por uma DAG no Airflow.
- **Máquinas Modernas (Linux Ubuntu):** Geram arquivos `.csv`. São sincronizados para a pasta temporária usando `rsync` com controle de *watermark* (leitura incremental) para monitoramento de alterações.
- **Processamento:** Scripts em Python leem os arquivos temporários, aplicam regras de negócio para tipagem e limpeza, garantem a unicidade e inserem os dados no PostgreSQL de forma segura.

### 3. Particionamento de Dados
- As tabelas de inspeção no banco de dados são **particionadas por data** para garantir performance nas consultas.
- Uma DAG específica no Airflow roda diariamente às 23:30 para provisionar automaticamente a partição do dia seguinte.

### 4. Estratégia de Backups
- **Backup Diário (Quente):** Um script no `crontab` gera um dump `.sql` do banco todos os dias às 02:00, salvo em uma partição dedicada do servidor.
- **Backup Trimestral (Frio/Data Lake):** Uma DAG do Airflow roda a cada 3 meses extraindo os dados históricos do PostgreSQL e salvando-os em formato colunar **Parquet**. O pipeline verifica a integridade do arquivo gerado e exclui as partições antigas do banco relacional, otimizando o armazenamento.

### 5. Orquestração e Observabilidade
- O Airflow centraliza todo o agendamento. Logs detalhados de cada task são registrados localmente para análise de falhas de rede ou de dados.
- O monitoramento do consumo de recursos do servidor e da saúde dos serviços é feito via **Prometheus e Grafana**.

### 6. Topologia de Rede e Segurança
- Por operar em um ambiente industrial, o servidor foi implementado em uma rede local restrita (Camada P2 / *Control Network*). Sistemas corporativos acessam os dados de forma controlada através de um gateway preexistente.
- O firewall do servidor (`ufw`/`iptables`) foi configurado estritamente para as portas necessárias:
  - `8080/tcp` - Airflow Web UI
  - `5050/tcp` - pgAdmin / PostgreSQL Web
  - `5432/tcp` - Conexão direta ao Database
  - `3000/tcp` - Grafana Dashboards
  - `22/tcp`   - Acesso remoto administrativo via SSH

---

## 🚀 Como Rodar

O processo foi desenhado para rodar de forma 100% autônoma como um pipeline ETL contínuo:
1. O Airflow busca os logs nos hosts originais.
2. Cria cópias em área de *staging* (temporária) no servidor.
3. Transforma e carrega (Upsert/Insert) os dados no PostgreSQL.
4. Exclui os arquivos temporários e atualiza os ponteiros de leitura.

Todo o gerenciamento é feito via interface web rodando no próprio servidor:
- **Airflow:** `http://localhost:8080`
- **PostgreSQL (Web):** `http://localhost:5050`
- **Grafana:** `http://localhost:3000`

*(Estes serviços podem ser acessados por outras máquinas da rede local industrial utilizando o IP fixo do servidor).*

### Requisitos do Ambiente
- Ubuntu 22.04+
- Python 3.10+
- PostgreSQL 14+
- Apache Airflow 2.9+