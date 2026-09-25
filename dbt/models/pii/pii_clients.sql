-- Clear-text client PII. This dataset is restricted to the compliance role;
-- use it only to address statements, never for analysis.
select
    client_id,
    full_name,
    email,
    country
from {{ ref('stg_clients') }}
