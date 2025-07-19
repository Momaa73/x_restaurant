# Bronze Layer Data Model

## Business Context
The bronze layer contains raw, unprocessed data ingested directly from source systems (e.g., reservation system, check-in system, customer feedback forms). This layer serves as the immutable source of truth, capturing all events as they arrive, including late or out-of-order data. No business logic or cleaning is applied at this stage.

### Table Descriptions
- **RESERVATIONS_RAW**: Each row represents a reservation made by a customer, including who made it, for which branch and table, when, and how many guests. This is the raw feed from the reservation system.
- **CHECKINS_RAW**: Each row represents a customer check-in at a branch, whether pre-booked or walk-in, including the time, table, and shift manager. This is the raw feed from the check-in system.
- **FEEDBACK_RAW**: Each row is a customer feedback entry, including the branch, customer, feedback text, rating, and when the feedback was submitted. This is the raw feed from feedback forms or digital surveys.

```mermaid
erDiagram
    RESERVATIONS_RAW {
        INT reservation_id
        STRING customer_name
        STRING phone_number
        INT branch_id
        INT table_id
        DATE reservation_date
        STRING reservation_hour
        INT guests_count
        TIMESTAMP created_at
        BOOLEAN limited_hours
        FLOAT hours_if_limited
    }
    CHECKINS_RAW {
        INT checkin_id
        STRING customer_name
        STRING phone_number
        INT branch_id
        INT table_id
        BOOLEAN is_prebooked
        DATE checkin_date
        STRING checkin_time
        INT guests_count
        STRING shift_manager
    }
    FEEDBACK_RAW {
        INT feedback_id
        INT branch_id
        STRING customer_name
        STRING phone_number
        STRING feedback_text
        INT rating
        DATE dining_date
        STRING dining_time
        TIMESTAMP submission_time
    }
    RESERVATIONS_RAW ||--o{ CHECKINS_RAW : "customer_name, phone_number"
    CHECKINS_RAW ||--o{ FEEDBACK_RAW : "customer_name, phone_number"
``` 