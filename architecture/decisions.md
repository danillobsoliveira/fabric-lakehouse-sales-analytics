# Architecture Decisions

This document describes the **key architectural and technical decisions** taken during the design and implementation of the **Fabric Lakehouse Medallion** project.  
The goal is to make explicit **why** each decision was made, what problems it solves, and which trade-offs were consciously accepted.

The project is designed as a **portfolio-grade implementation**, prioritizing architectural clarity, best practices, and reasoning transparency over production-scale complexity.

---

## Architecture Choice

**Decision**: Adopt a **Medallion Architecture (Bronze → Silver → Gold)** implemented within a **single Microsoft Fabric Lakehouse**, using logical separation by schemas.

**Rationale**:
- Medallion is a well-established architectural pattern for modern Lakehouse platforms.
- Clear separation of concerns improves maintainability, governance, and data lineage.
- Logical separation via schemas aligns with Microsoft Fabric best practices and reduces operational overhead.
- Simplifies navigation and discovery for analytics and BI workloads.

**Trade-offs and Limitations**:
- No physical isolation between layers; relies on naming conventions and access discipline.
- Not suitable for strict multi-tenant or highly regulated isolation requirements without further controls.

---

## Data Ingestion Strategy

**Decision**: Use **batch ingestion** of nested **JSON files** stored in the Lakehouse `Files` area, persisting ingested data as **Delta tables** in the Bronze layer.

**Rationale**:
- Nested JSON reflects realistic transactional and API-based source systems.
- Preserves source structure and semantics in the Bronze layer.
- Decouples data generation from ingestion, enabling idempotent reprocessing.
- Delta Lake provides ACID guarantees, schema enforcement, and time travel.

**Trade-offs and Limitations**:
- Synthetic data does not represent all real-world data anomalies.
- No incremental ingestion logic (CDC, watermarking) implemented.

---

## Batch vs Streaming

**Decision**: Implement the entire pipeline using **batch processing only**.

**Rationale**:
- The project focuses on analytical architecture and modeling, not low-latency ingestion.
- Batch processing simplifies reprocessing, debugging, and architectural understanding.
- Avoids additional complexity related to streaming state, late-arriving data, and event-time handling.

**Trade-offs and Limitations**:
- Near real-time use cases are not supported.
- Streaming ingestion could be added later as an extension (e.g., Eventstream → Bronze).

---

## Data Modeling Approach

**Decision**: Implement **dimensional modeling (Kimball)** in the Gold layer using a **Star Schema** with:
- Append-only fact tables
- Conformed dimensions
- Slowly Changing Dimensions Type 2 (SCD2)

**Rationale**:
- Dimensional models are widely understood and optimized for BI tools.
- Star Schema simplifies analytical queries and aggregations.
- SCD Type 2 preserves historical context and enables point-in-time analysis.
- Append-only facts improve auditability and simplify lineage.

**Trade-offs and Limitations**:
- Increased modeling and storage complexity compared to flat models.
- Requires careful surrogate key management.
- Not optimized for highly volatile transactional updates.

---

## Surrogate Key Strategy

**Decision**: Generate surrogate keys manually in Spark instead of using identity or auto-increment columns.

**Rationale**:
- Microsoft Fabric Lakehouse does not reliably support identity columns in Spark-managed tables.
- Manual surrogate key generation ensures deterministic and controlled key assignment.
- Supports idempotent batch execution.

**Trade-offs and Limitations**:
- Requires explicit logic and ordering guarantees.
- Additional complexity compared to database-managed identity columns.

---

## Data Quality and Standardization

**Decision**: Centralize data cleaning, validation, and standardization logic in the **Silver layer**.

**Rationale**:
- Establishes Silver as the semantic and quality boundary of the platform.
- Prevents raw ingestion logic from becoming polluted with business rules.
- Ensures the Gold layer receives stable, analytics-ready datasets.

**Trade-offs and Limitations**:
- Validation rules are basic and illustrative.
- No dedicated data quality framework or persisted metrics implemented.

---

## Tooling and Technologies

**Decision**: Use a **Microsoft Fabric–centric stack**:
- Fabric Lakehouse + Delta Lake
- Spark Notebooks (PySpark)
- Fabric Data Pipelines
- Git for version control

**Rationale**:
- Demonstrates hands-on expertise with the Microsoft Fabric ecosystem.
- Tight integration between storage, compute, orchestration, and governance.
- Reduced operational complexity for analytical workloads.

**Trade-offs and Limitations**:
- Strong coupling to the Microsoft ecosystem.
- Some Spark runtime limitations compared to open-source environments.

---

## Orchestration Strategy

**Decision**: Use a **manually triggered Fabric Data Pipeline** without schedules or alerts.

**Rationale**:
- Keeps the project focused on architecture rather than operations.
- Simplifies execution flow for demonstration and review.
- Avoids introducing operational noise not relevant to a portfolio.

**Trade-offs and Limitations**:
- No automated execution or SLA guarantees.
- Monitoring and alerting are out of scope.

---

## Summary of Trade-offs

The following limitations are **intentional**:

- No CI/CD or deployment automation
- No streaming or CDC ingestion
- Manual pipeline execution
- Synthetic data only
- Fabric-specific implementation

These decisions were made to prioritize **architectural clarity, correctness, and explanatory power**, which are the primary goals of this project.
