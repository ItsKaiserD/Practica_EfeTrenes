# Arquitectura MCP – Sprint 1

Este documento describe la arquitectura objetivo alineada a **Sprint 1 (04–23 de noviembre de 2025)**, roles y HU del backlog.

## Decisiones por capa

- **Ingesta & Orquestación** (HU1, HU2): Azure Data Factory (ADF) **o** Apache Airflow para planificar/ejecutar pipelines. FastAPI estandariza la entrada de datos externos con contratos **JSON/CSV** y autenticación (token/Entra ID).
- **Data Lake (ADLS Gen2)** (HU4, HU6):
  - `raw`: aterrizaje por fuente/fecha, formatos CSV/JSON/Parquet.
  - `silver`: datos validados/limpios (tests, tipado, catálogos).
  - `gold`: modelo estrella (dimensiones y hechos) para consumo analítico.
  - **Delta Lake** habilita versionado, time travel y rollback.
- **Transformación & Cálculo MCP** (HU3): Notebooks/PySpark y/o dbt para transformar y calcular indicadores MCP de forma idempotente, con **logs** de ejecución y opción de **recalcular**.
- **Exposición Analítica** (HU4, HU5): Capa SQL opcional (Synapse/SQL DB) y **Power BI** con **RLS** e **Incremental Refresh**, filtros por fecha/filial/servicio/tipo y tiempos <5s.
- **Gobernanza, Seguridad y Observabilidad** (HU7, HU8): Purview (catálogo y linaje), Entra ID + RBAC, secretos en **Key Vault**, y métricas/alertas con **Log Analytics/Monitor**.

## Naming, ambientes y rutas
- Ambientes: `dev`, `qa`, `prod` (RG, ADLS, ADF/Airflow, Key Vault y Databricks por entorno).
- Naming: `mcp-<env>-<recurso>` (ej: `mcp-dev-adls`, `mcp-qa-kv`).
- Rutas ADLS:
  - `raw/<fuente>/YYYY=YYYY/MM=MM/DD=DD/*.csv|json`
  - `silver/<dominio>/<tabla>/...`
  - `gold/bi/<entidad>/...`

## HU cubiertas en Sprint 1
- **HU1/HU2**: Ingesta interna/externa automatizada con contratos y validación de no-pérdida.
- **HU3**: Cálculo MCP automatizado con logs y capacidad de recalcular.
- **HU4/HU5**: Dataset de PBI con refresh semanal (o diario) y filtros clave.
- **HU6**: Versionado Delta y rollback probado.
- **HU7**: 3 roles base (lector, editor, admin) + bitácora de accesos.
- **HU8**: Comité de gobernanza, políticas y catálogo de fuentes en Purview.

---

## Archivos
- Este `README` – explicación destinada al repositorio de documentación.
