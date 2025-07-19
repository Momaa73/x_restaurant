# Data Quality Checks

This document outlines the data quality validation rules, checks, and standards implemented in the restaurant data pipeline using Great Expectations.

## Overview

Data quality checks are implemented at each layer of the data pipeline (Bronze, Silver, Gold) to ensure data integrity, completeness, accuracy, and consistency. These checks are automated and run as part of the data pipeline orchestrated by Apache Airflow.

**Note on Implementation**: While we attempted to implement automated data quality checks using Great Expectations, we encountered compatibility issues between Great Expectations and our specific Spark/Iceberg setup. The validation rules and framework outlined in this document represent the intended data quality standards, but the automated execution requires additional configuration and troubleshooting to resolve the integration challenges.

---

---

## Quality Check Categories

### 1. Schema Validation
Ensures data structure matches expected schema and data types.

### 2. Data Completeness
Validates that required fields are not null and data is not missing.

### 3. Data Accuracy
Checks data values against business rules and expected ranges.

### 4. Data Consistency
Ensures data is consistent across tables and time periods.

### 5. Business Logic Validation
Validates business-specific rules and relationships.

---

## Bronze Layer Validations

### Schema Validation
```python
# Check required columns exist
expect_table_columns_to_match_set(
    column_set=[
        "reservation_id", "customer_id", "reservation_time", 
        "holiday", "raw_notes"
    ]
)

# Validate data types
expect_column_values_to_be_of_type("reservation_id", "str")
expect_column_values_to_be_of_type("customer_id", "str")
expect_column_values_to_be_of_type("reservation_time", "str")
expect_column_values_to_be_of_type("holiday", "str")
expect_column_values_to_be_of_type("raw_notes", "str")
```

### Data Completeness
```python
# Check for null values in key fields
expect_column_values_to_not_be_null("reservation_id")
expect_column_values_to_not_be_null("customer_id")
expect_column_values_to_not_be_null("reservation_time")

# Check for empty strings
expect_column_values_to_not_match_regex("reservation_id", r"^\s*$")
expect_column_values_to_not_match_regex("customer_id", r"^\s*$")
```

### Data Accuracy
```python
# Validate timestamp format
expect_column_values_to_match_regex(
    "reservation_time", 
    r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
)

# Check for reasonable date ranges
expect_column_values_to_be_between(
    "reservation_time",
    min_value="2024-01-01 00:00:00",
    max_value="2025-12-31 23:59:59"
)
```

### Business Logic
```python
# Ensure reservation IDs are unique
expect_compound_columns_to_be_unique(["reservation_id"])

# Check for reasonable data volume
expect_table_row_count_to_be_between(1, 100000)
```

---

## Silver Layer Validations

### Schema Validation
```python
# Check cleaned table structure
expect_table_columns_to_match_set(
    column_set=[
        "reservation_id", "customer_id", "reservation_time", 
        "is_holiday", "notes", "cleaned_at"
    ]
)

# Validate data types after cleaning
expect_column_values_to_be_of_type("reservation_id", "str")
expect_column_values_to_be_of_type("customer_id", "str")
expect_column_values_to_be_of_type("reservation_time", "datetime64[ns]")
expect_column_values_to_be_of_type("is_holiday", "bool")
expect_column_values_to_be_of_type("notes", "str")
expect_column_values_to_be_of_type("cleaned_at", "datetime64[ns]")
```

### Data Completeness
```python
# All key fields should be populated after cleaning
expect_column_values_to_not_be_null("reservation_id")
expect_column_values_to_not_be_null("customer_id")
expect_column_values_to_not_be_null("reservation_time")
expect_column_values_to_not_be_null("is_holiday")
expect_column_values_to_not_be_null("cleaned_at")

# Notes can be null but if present should not be empty
expect_column_values_to_not_match_regex("notes", r"^\s*$", mostly=0.9)
```

### Data Accuracy
```python
# Validate boolean values
expect_column_values_to_be_in_set("is_holiday", [True, False])

# Check timestamp is in reasonable range
expect_column_values_to_be_between(
    "reservation_time",
    min_value="2024-01-01 00:00:00",
    max_value="2025-12-31 23:59:59"
)

# Ensure cleaning timestamp is recent
expect_column_values_to_be_between(
    "cleaned_at",
    min_value="2024-01-01 00:00:00",
    max_value="2025-12-31 23:59:59"
)
```

### Data Consistency
```python
# Check referential integrity with bronze layer
expect_column_values_to_be_in_set(
    "reservation_id",
    value_set=bronze_reservations_df["reservation_id"].tolist()
)

# Ensure no duplicates after cleaning
expect_compound_columns_to_be_unique(["reservation_id"])
```

---

## Gold Layer Validations

### Schema Validation
```python
# Check fact table structure
expect_table_columns_to_match_set(
    column_set=[
        "reservation_id", "customer_id", "reservation_time", 
        "is_holiday", "total_checkins", "avg_rating", 
        "ml_prediction", "created_at"
    ]
)

# Validate aggregated data types
expect_column_values_to_be_of_type("reservation_id", "str")
expect_column_values_to_be_of_type("customer_id", "str")
expect_column_values_to_be_of_type("reservation_time", "datetime64[ns]")
expect_column_values_to_be_of_type("is_holiday", "bool")
expect_column_values_to_be_of_type("total_checkins", "int64")
expect_column_values_to_be_of_type("avg_rating", "float64")
expect_column_values_to_be_of_type("ml_prediction", "str")
expect_column_values_to_be_of_type("created_at", "datetime64[ns]")
```

### Data Completeness
```python
# All fields should be populated in gold layer
expect_column_values_to_not_be_null("reservation_id")
expect_column_values_to_not_be_null("customer_id")
expect_column_values_to_not_be_null("reservation_time")
expect_column_values_to_not_be_null("is_holiday")
expect_column_values_to_not_be_null("total_checkins")
expect_column_values_to_not_be_null("avg_rating")
expect_column_values_to_not_be_null("created_at")

# ML prediction can be null (placeholder for future implementation)
expect_column_values_to_be_null("ml_prediction", mostly=1.0)
```

### Data Accuracy
```python
# Validate aggregated values
expect_column_values_to_be_between("total_checkins", 0, 100)
expect_column_values_to_be_between("avg_rating", 0.0, 5.0)

# Check boolean values
expect_column_values_to_be_in_set("is_holiday", [True, False])

# Validate timestamp ranges
expect_column_values_to_be_between(
    "reservation_time",
    min_value="2024-01-01 00:00:00",
    max_value="2025-12-31 23:59:59"
)
```

### Business Logic
```python
# Ensure unique reservations
expect_compound_columns_to_be_unique(["reservation_id"])

# Check data volume is reasonable
expect_table_row_count_to_be_between(1, 50000)

# Validate aggregation logic
expect_column_values_to_be_between("total_checkins", 0, 10)
expect_column_values_to_be_between("avg_rating", 1.0, 5.0)
```

---

## Cross-Layer Validations

### Data Lineage Checks
```python
# Ensure all bronze records have corresponding silver records
expect_column_values_to_be_in_set(
    bronze_df["reservation_id"],
    value_set=silver_df["reservation_id"].tolist()
)

# Ensure all silver records have corresponding gold records
expect_column_values_to_be_in_set(
    silver_df["reservation_id"],
    value_set=gold_df["reservation_id"].tolist()
)
```

### Consistency Checks
```python
# Check that holiday flags are consistent
expect_column_values_to_be_in_set(
    silver_df["is_holiday"],
    value_set=gold_df["is_holiday"].tolist()
)

# Validate that timestamps are preserved correctly
expect_column_values_to_be_in_set(
    silver_df["reservation_time"],
    value_set=gold_df["reservation_time"].tolist()
)
```

---

## Great Expectations Configuration

### Expectation Suite Setup
```python
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest

# Initialize Great Expectations context
context = ge.get_context()

# Create expectation suite
suite = context.create_expectation_suite(
    expectation_suite_name="restaurant_data_quality",
    overwrite_existing=True
)

# Add expectations to suite
validator = context.get_validator(
    batch_request=RuntimeBatchRequest(
        datasource_name="spark_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="bronze_reservations",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "default_identifier"},
    ),
    expectation_suite=suite,
)
```

### Checkpoint Configuration
```python
# Create checkpoint for automated validation
checkpoint = context.add_or_update_checkpoint(
    name="restaurant_data_quality_checkpoint",
    validator=validator,
    expectation_suite_name="restaurant_data_quality",
    action_list=[
        {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"},
        },
        {
            "name": "store_evaluation_params",
            "action": {"class_name": "StoreEvaluationParametersAction"},
        },
        {
            "name": "update_data_docs",
            "action": {"class_name": "UpdateDataDocsAction"},
        },
    ],
)
```

---

## Airflow Integration

### DAG Task for Data Quality
```python
from airflow.operators.python_operator import PythonOperator
import great_expectations as ge

def run_data_quality_checks(**context):
    """Run Great Expectations data quality checks"""
    
    # Initialize context
    ge_context = ge.get_context()
    
    # Run validations for each layer
    bronze_result = ge_context.run_checkpoint(
        checkpoint_name="bronze_data_quality_checkpoint"
    )
    
    silver_result = ge_context.run_checkpoint(
        checkpoint_name="silber_data_quality_checkpoint"
    )
    
    gold_result = ge_context.run_checkpoint(
        checkpoint_name="gold_data_quality_checkpoint"
    )
    
    # Check if all validations passed
    if not all([bronze_result.success, silver_result.success, gold_result.success]):
        raise ValueError("Data quality checks failed")
    
    return "Data quality checks passed"

# Add to DAG
data_quality_task = PythonOperator(
    task_id='run_data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag
)
```

---

## Alerting and Monitoring

### Failure Alerts
```python
# Configure alerts for data quality failures
def alert_data_quality_failure(**context):
    """Send alert when data quality checks fail"""
    
    # Get failure details
    task_instance = context['task_instance']
    failure_reason = task_instance.xcom_pull(task_ids='run_data_quality_checks')
    
    # Send alert (email, Slack, etc.)
    send_alert(
        subject="Data Quality Check Failed",
        message=f"Data quality validation failed: {failure_reason}"
    )

# Add alert task to DAG
alert_task = PythonOperator(
    task_id='alert_data_quality_failure',
    python_callable=alert_data_quality_failure,
    trigger_rule='one_failed',
    dag=dag
)
```

### Quality Metrics Dashboard
```python
# Track quality metrics over time
def log_quality_metrics(**context):
    """Log data quality metrics for monitoring"""
    
    # Calculate quality scores
    bronze_score = calculate_quality_score("bronze")
    silver_score = calculate_quality_score("silver")
    gold_score = calculate_quality_score("gold")
    
    # Log metrics
    log_metric("bronze_quality_score", bronze_score)
    log_metric("silver_quality_score", silver_score)
    log_metric("gold_quality_score", gold_score)
```

---

## Quality Score Calculation

### Scoring Methodology
```python
def calculate_quality_score(layer_name):
    """Calculate overall quality score for a data layer"""
    
    scores = {
        "schema_validation": 0.25,
        "data_completeness": 0.25,
        "data_accuracy": 0.25,
        "business_logic": 0.25
    }
    
    # Calculate weighted average
    total_score = sum(scores.values())
    
    return total_score / len(scores)
```

### Quality Thresholds
- **Excellent**: 95-100%
- **Good**: 85-94%
- **Acceptable**: 75-84%
- **Poor**: <75%

---

## Maintenance and Updates

### Regular Review Schedule
- **Daily**: Automated quality checks run with each pipeline execution
- **Weekly**: Review quality metrics and trends
- **Monthly**: Update validation rules based on business changes
- **Quarterly**: Comprehensive quality assessment and rule optimization

### Rule Updates
```python
# Example of updating validation rules
def update_validation_rules():
    """Update validation rules based on business requirements"""
    
    # Add new validation for new business rule
    validator.expect_column_values_to_be_between(
        "new_field",
        min_value=0,
        max_value=100
    )
    
    # Remove outdated validation
    # validator.expect_column_values_to_match_regex("old_field", r"old_pattern")
```

---

## Documentation and Reporting

### Quality Reports
- **Automated Reports**: Generated after each validation run
- **Trend Analysis**: Track quality metrics over time
- **Issue Tracking**: Document and track quality issues
- **Resolution Tracking**: Monitor issue resolution progress

### Data Quality Documentation
- **Validation Rules**: Document all validation rules and their business justification
- **Quality Metrics**: Track and report on quality metrics
- **Issue Log**: Maintain log of quality issues and resolutions
- **Best Practices**: Document data quality best practices and guidelines

---

## Summary

This data quality framework ensures:

1. **Automated Validation**: All quality checks run automatically as part of the pipeline
2. **Comprehensive Coverage**: Validations cover schema, completeness, accuracy, and business logic
3. **Cross-Layer Consistency**: Ensures data consistency across Bronze, Silver, and Gold layers
4. **Alerting and Monitoring**: Immediate notification of quality issues
5. **Documentation**: Comprehensive documentation of all quality rules and metrics
6. **Maintenance**: Regular review and updates of quality rules

**The data quality checks are essential for maintaining data integrity and ensuring reliable analytics and reporting.**
