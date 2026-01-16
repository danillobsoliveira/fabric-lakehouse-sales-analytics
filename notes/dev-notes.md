# Fabric Lakehouse Medallion – Development Notes

> **Artifact: medallion_env**

## 2026-01-15
- **Decision**: Created a dedicated Spark environment (`medallion_env`) for the project.  
- **Reason**: Ensure reproducibility, dependency isolation, and consistent execution across notebooks.  
- **Limitation**: Environment-level changes require restarting Spark sessions.

- **Decision**: Pinned `faker==40.1.2` for synthetic data generation.  
- **Reason**: Enable deterministic simulation of transactional datasets for Bronze ingestion.  
- **Limitation**: Synthetic data cannot cover all real-world edge cases.

---

> **Artifact: dataset_generator (Bronze)**

## 2026-01-15
- **Decision**: Modeled purchase events as nested JSON with embedded customer and product info.  
- **Reason**: Reflect real-world transactional APIs and event-based ingestion.  
- **Limitation**: Bronze layer contains redundant data, requiring normalization in Silver.

- **Decision**: Persisted generated datasets in Lakehouse `/Files` path.  
- **Reason**: Decouple generation from ingestion, enabling repeatable, independent execution.  
- **Limitation**: Adds intermediate storage step; in production, data may come from direct sources.

---

> **Artifact: bronze_ingestion**

## 2026-01-15
- **Decision**: Added technical metadata columns (ingestion timestamp, source system, batch_id).  
- **Reason**: Enable lineage, auditability, and observability.  
- **Limitation**: Metadata alone is insufficient for full data quality monitoring.

- **Decision**: Exploded nested purchase arrays to one row per purchase event.  
- **Reason**: Improve queryability and downstream processing.  
- **Limitation**: Slightly blurs the boundary between raw ingestion and Silver modeling.

---

> **Artifact: silver_transformation**

## 2026-01-15
- **Decision**: Normalized, cleaned, standardized, and enriched Bronze datasets.  
- **Reason**: Produce analytics-ready datasets and isolate structural/semantic transformations from raw ingestion.  
- **Limitation**: Dependent on stable upstream schemas; changes require pipeline adjustments.

---

> **Artifact: gold_modeling**

## 2026-01-15
- **Decision**: Implemented dimensional modeling (star schema) with fact and dimension tables.  
- **Reason**: Optimize analytical queries, simplify aggregations, and align with BI patterns.  
- **Limitation**: Dimensions require maintenance when business entities evolve.

- **Decision**: Implemented SCD Type 2 in dimensions using hash-based change detection.  
- **Reason**: Preserve historical context and enable point-in-time analysis.  
- **Limitation**: Increases storage and complexity; hash collisions possible but unlikely.

- **Decision**: Avoided identity/auto-increment surrogate keys.  
- **Reason**: Lakehouse limitations make identity columns unreliable at scale.  
- **Limitation**: Surrogate key generation must be handled explicitly in transformations.

- **Decision**: Fact tables are append-only.  
- **Reason**: Maintain immutable facts, simplify lineage and auditing.  
- **Limitation**: Corrections require compensating records rather than in-place updates.

- **Decision**: Modeled `dim_date` with integer surrogate key `YYYYMMDD` format, immutable.  
- **Reason**: Deterministic joins, query performance, avoids mutable date attributes.  
- **Limitation**: Date corrections require full regeneration.

---

> **Artifact: Lakehouse (Architecture)**

## 2026-01-15
- **Decision**: Used a single Lakehouse for Bronze, Silver, Gold layers, separated by schemas.  
- **Reason**: Simplifies governance, reduces operational overhead, and aligns with Fabric best practices.  
- **Limitation**: Requires strict naming conventions and disciplined access control.

---

> **Artifact: Data Pipeline (Orchestration)**

## 2026-01-15
- **Decision**: Pipeline is manual, without triggers or schedules.  
- **Reason**: Portfolio demonstration, no production-grade execution required.  
- **Limitation**: Does not include automated monitoring, alerting, or SLA enforcement.
