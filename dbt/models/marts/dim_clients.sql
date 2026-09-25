-- Client dimension with no clear-text PII. Names are masked to initials and
-- emails are hashed, so analysts can join and count clients without seeing
-- who they are. Clear text lives only in pii.pii_clients.
select
    client_id,
    {{ mask_name('full_name') }} as display_name_masked,
    to_hex(sha256(email)) as email_sha256,
    country,
    account_currency,
    risk_tier,
    opened_at,
    updated_at
from {{ ref('stg_clients') }}
