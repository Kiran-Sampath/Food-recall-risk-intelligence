create table if not exists recalls (
  recall_number text primary key,
  event_id text,
  status text,
  classification text,
  product_type text,
  recalling_firm text,
  recalling_firm_clean text,
  product_description text,
  reason_for_recall text,
  recall_reason_category text,
  product_category text,
  distribution_pattern text,
  distribution_scope text,
  state text,
  region text,
  country text,
  city text,
  postal_code text,
  product_quantity text,
  voluntary_mandated text,
  initial_firm_notification text,
  recall_initiation_date date,
  report_date date,
  termination_date date,
  is_open boolean,
  recall_duration_days integer,
  repeated_company_count integer,
  classification_score integer,
  status_score integer,
  distribution_score integer,
  reason_score integer,
  repeated_company_score integer,
  risk_score integer,
  risk_tier text,
  risk_explanation text,
  updated_at timestamptz default now()
);

create table if not exists pipeline_runs (
  id bigint generated always as identity primary key,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  source text,
  bronze_path text,
  records_fetched integer,
  records_loaded integer,
  status text not null,
  warning text,
  error text
);

create table if not exists company_risk (
  recalling_firm_clean text primary key,
  total_recalls integer,
  avg_risk_score double precision,
  latest_recall_date date,
  class_i_recalls integer,
  high_risk_recalls integer,
  ongoing_recalls integer,
  updated_at timestamptz default now()
);

create table if not exists recall_reason_summary (
  recall_reason_category text primary key,
  count integer,
  updated_at timestamptz default now()
);

create table if not exists monthly_recall_trends (
  recall_month text primary key,
  count integer,
  updated_at timestamptz default now()
);

create table if not exists geographic_recall_summary (
  state text,
  country text,
  total_recalls integer,
  avg_risk_score double precision,
  updated_at timestamptz default now(),
  primary key (state, country)
);

create table if not exists product_category_summary (
  product_category text primary key,
  count integer,
  avg_risk_score double precision,
  updated_at timestamptz default now()
);

create table if not exists risk_tier_summary (
  risk_tier text primary key,
  count integer,
  avg_risk_score double precision,
  updated_at timestamptz default now()
);

create table if not exists open_recall_aging (
  recall_number text primary key,
  recalling_firm_clean text,
  classification text,
  recall_reason_category text,
  recall_initiation_date date,
  recall_duration_days integer,
  risk_score integer,
  risk_tier text,
  updated_at timestamptz default now()
);

create index if not exists idx_recalls_report_date on recalls (report_date);
create index if not exists idx_recalls_risk_tier on recalls (risk_tier);
create index if not exists idx_recalls_company on recalls (recalling_firm_clean);
create index if not exists idx_recalls_state on recalls (state);
create index if not exists idx_recalls_product_category on recalls (product_category);
