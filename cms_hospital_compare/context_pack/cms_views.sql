-- CMS Hospital Compare context pack: typed DuckDB views over the cms_hospital_compare connector's tables.
--
-- The connector lands every CMS value as text, exactly as the API sends it, and keeps every release it loads.
-- These views give an agent one typed, current answer per question:
--   1. Each <table>_history view reads only releases that dataset_release lists as completely loaded, so a release
--      that is still loading is never read. It drops rows with _fivetran_deleted = true.
--   2. Each plain view (cms_hospital, cms_hrrp, ...) keeps only the latest complete release of its table.
--   3. Numbers are cast with try_cast, so every CMS sentinel ("Not Available", "Not Applicable", "N/A",
--      "Too Few to Report", an empty string) becomes NULL instead of a string that sorts or averages wrongly.
--   4. Dates become DATE: cms_released from YYYY-MM-DD, start_date and end_date from MM/DD/YYYY.
--
-- Column names match the connector's tables. Where a cast changes what a column holds, the text CMS sent is kept
-- beside it with a _text suffix.
--
-- The views name the connector's tables without a schema, so the same file runs over any copy of them. Put the
-- schema that holds the tables on the search path after memory.main, then read this file:
--
--   duckdb
--   ATTACH '../connector/files/warehouse.db' AS cms_wh (READ_ONLY);
--   SET search_path = 'memory.main,cms_wh.tester';
--   .read cms_views.sql
--
-- fivetran debug writes to the schema tester. A deployed connection writes to a schema named after the
-- connection; on a Fivetran Managed Data Lake, attach the catalog and use <lake_catalog_alias>.<connection_schema>.
-- The views are created in the in-memory database, so the warehouse stays read-only.

-- Every release that loaded completely, one row per table per release. is_latest_release marks the release the
-- plain views read.
--
-- The connector only ever loads a dataset's current release, so the latest load is the latest release. Ties fall
-- to release_version: the data file's Unix epoch, or the catalog's %modified timestamp
-- (2026-08-13T01:41:07-0400) when CMS publishes no file version, both read as epoch seconds. The two are not one
-- clock (hrrp's file version is 2026-02-03, its %modified 2026-08-13), so the version never outranks load order.
create or replace view cms_release_history as
with releases as (
  select
    dataset_id,
    release_version,
    table_name,
    try_cast(cms_released as date) as cms_released,
    cms_modified,
    row_count,
    api_row_count,
    loaded_at,
    coalesce(
      try_cast(release_version as bigint),
      cast(epoch(try_cast(release_version as timestamptz)) as bigint)
    ) as release_epoch
  from dataset_release
  where not coalesce(_fivetran_deleted, false)
)

select
  dataset_id,
  release_version,
  table_name,
  cms_released,
  cms_modified,
  row_count,
  api_row_count,
  loaded_at,
  row_number() over (
    partition by table_name
    order by loaded_at desc nulls last, release_epoch desc nulls last
  ) = 1 as is_latest_release
from releases;

create or replace view cms_release as
select * exclude (is_latest_release)
from cms_release_history
where is_latest_release;

-- Hospital General Information (xubh-q36u): one row per Medicare-certified hospital per release.
create or replace view cms_hospital_history as
select
  h.facility_id,
  h.facility_name,
  h.address,
  h.citytown,
  h.state,
  h.zip_code,
  h.countyparish,
  h.telephone_number,
  h.hospital_type,
  h.hospital_ownership,
  h.emergency_services = 'Yes' as has_emergency_services,
  h.meets_criteria_for_birthing_friendly_designation = 'Y' as is_birthing_friendly,
  try_cast(h.hospital_overall_rating as integer) as hospital_overall_rating,
  h.hospital_overall_rating_footnote,
  try_cast(h.mort_group_measure_count as integer) as mort_group_measure_count,
  try_cast(h.count_of_facility_mort_measures as integer) as count_of_facility_mort_measures,
  try_cast(h.count_of_mort_measures_better as integer) as count_of_mort_measures_better,
  try_cast(h.count_of_mort_measures_no_different as integer) as count_of_mort_measures_no_different,
  try_cast(h.count_of_mort_measures_worse as integer) as count_of_mort_measures_worse,
  h.mort_group_footnote,
  try_cast(h.safety_group_measure_count as integer) as safety_group_measure_count,
  try_cast(h.count_of_facility_safety_measures as integer) as count_of_facility_safety_measures,
  try_cast(h.count_of_safety_measures_better as integer) as count_of_safety_measures_better,
  try_cast(h.count_of_safety_measures_no_different as integer) as count_of_safety_measures_no_different,
  try_cast(h.count_of_safety_measures_worse as integer) as count_of_safety_measures_worse,
  h.safety_group_footnote,
  try_cast(h.readm_group_measure_count as integer) as readm_group_measure_count,
  try_cast(h.count_of_facility_readm_measures as integer) as count_of_facility_readm_measures,
  try_cast(h.count_of_readm_measures_better as integer) as count_of_readm_measures_better,
  try_cast(h.count_of_readm_measures_no_different as integer) as count_of_readm_measures_no_different,
  try_cast(h.count_of_readm_measures_worse as integer) as count_of_readm_measures_worse,
  h.readm_group_footnote,
  try_cast(h.pt_exp_group_measure_count as integer) as pt_exp_group_measure_count,
  try_cast(h.count_of_facility_pt_exp_measures as integer) as count_of_facility_pt_exp_measures,
  h.pt_exp_group_footnote,
  try_cast(h.te_group_measure_count as integer) as te_group_measure_count,
  try_cast(h.count_of_facility_te_measures as integer) as count_of_facility_te_measures,
  h.te_group_footnote,
  h.cms_release_version,
  try_cast(h.cms_released as date) as cms_released,
  r.is_latest_release
from hospital as h
inner join cms_release_history as r
  on r.table_name = 'hospital'
  and r.release_version = h.cms_release_version
where not coalesce(h._fivetran_deleted, false);

create or replace view cms_hospital as
select * exclude (is_latest_release)
from cms_hospital_history
where is_latest_release;

-- Timely and Effective Care (yv7e-xc69): one row per hospital per measure per release. score is NULL for the
-- EDV volume band; score_text keeps the band (low, medium, high, very high).
create or replace view cms_timely_effective_care_history as
select
  t.facility_id,
  t.facility_name,
  t.state,
  t.measure_id,
  t.measure_name,
  t.measure_condition,
  try_cast(t.score as double) as score,
  t.score as score_text,
  try_cast(t.sample_size as integer) as sample_size,
  t.footnote,
  cast(try_strptime(t.start_date, '%m/%d/%Y') as date) as start_date,
  cast(try_strptime(t.end_date, '%m/%d/%Y') as date) as end_date,
  t.cms_release_version,
  try_cast(t.cms_released as date) as cms_released,
  r.is_latest_release
from timely_effective_care as t
inner join cms_release_history as r
  on r.table_name = 'timely_effective_care'
  and r.release_version = t.cms_release_version
where not coalesce(t._fivetran_deleted, false);

create or replace view cms_timely_effective_care as
select * exclude (is_latest_release)
from cms_timely_effective_care_history
where is_latest_release;

-- Unplanned Hospital Visits (632h-zaca): one row per hospital per measure per release. score is a rate in percent
-- for READM_30_*, days per 100 discharges for EDAC_30_*, and a rate or ratio for the OP_* measures.
-- comparison folds the case variants CMS uses in compared_to_national into five values.
create or replace view cms_unplanned_hospital_visit_history as
select
  u.facility_id,
  u.facility_name,
  u.state,
  u.measure_id,
  u.measure_name,
  u.compared_to_national,
  case
    when lower(u.compared_to_national) like 'better than%' then 'better'
    when lower(u.compared_to_national) like 'no different than%' then 'no_different'
    when lower(u.compared_to_national) like 'worse than%' then 'worse'
    when lower(u.compared_to_national) = 'number of cases too small' then 'too_few_cases'
    else 'not_available'
  end as comparison,
  try_cast(u.score as double) as score,
  try_cast(u.denominator as integer) as denominator,
  try_cast(u.lower_estimate as double) as lower_estimate,
  try_cast(u.higher_estimate as double) as higher_estimate,
  try_cast(u.number_of_patients as integer) as number_of_patients,
  try_cast(u.number_of_patients_returned as integer) as number_of_patients_returned,
  u.footnote,
  cast(try_strptime(u.start_date, '%m/%d/%Y') as date) as start_date,
  cast(try_strptime(u.end_date, '%m/%d/%Y') as date) as end_date,
  u.cms_release_version,
  try_cast(u.cms_released as date) as cms_released,
  r.is_latest_release
from unplanned_hospital_visit as u
inner join cms_release_history as r
  on r.table_name = 'unplanned_hospital_visit'
  and r.release_version = u.cms_release_version
where not coalesce(u._fivetran_deleted, false);

create or replace view cms_unplanned_hospital_visit as
select * exclude (is_latest_release)
from cms_unplanned_hospital_visit_history
where is_latest_release;

-- National rates for the same measures (cvcs-xecj): one row per measure per release. The READM_30_* and OP_32/35
-- measures use the better/same/worse/too_few counts; OP_36 and EDAC_30_* use the fewer/average/more/too_small counts.
create or replace view cms_unplanned_hospital_visit_national_history as
select
  n.measure_id,
  n.measure_name,
  try_cast(n.national_rate as double) as national_rate,
  try_cast(n.number_of_hospitals_better as integer) as number_of_hospitals_better,
  try_cast(n.number_of_hospitals_same as integer) as number_of_hospitals_same,
  try_cast(n.number_of_hospitals_worse as integer) as number_of_hospitals_worse,
  try_cast(n.number_of_hospitals_too_few as integer) as number_of_hospitals_too_few,
  try_cast(n.number_of_hospitals_fewer as integer) as number_of_hospitals_fewer,
  try_cast(n.number_of_hospitals_average as integer) as number_of_hospitals_average,
  try_cast(n.number_of_hospitals_more as integer) as number_of_hospitals_more,
  try_cast(n.number_of_hospitals_too_small as integer) as number_of_hospitals_too_small,
  n.footnote,
  cast(try_strptime(n.start_date, '%m/%d/%Y') as date) as start_date,
  cast(try_strptime(n.end_date, '%m/%d/%Y') as date) as end_date,
  n.cms_release_version,
  try_cast(n.cms_released as date) as cms_released,
  r.is_latest_release
from unplanned_hospital_visit_national as n
inner join cms_release_history as r
  on r.table_name = 'unplanned_hospital_visit_national'
  and r.release_version = n.cms_release_version
where not coalesce(n._fivetran_deleted, false);

create or replace view cms_unplanned_hospital_visit_national as
select * exclude (is_latest_release)
from cms_unplanned_hospital_visit_national_history
where is_latest_release;

-- Hospital Readmissions Reduction Program (9n3s-kdb3): one row per hospital per condition per release.
-- excess_readmission_ratio, in CMS's words: "calculated by dividing a hospital's predicted rate of readmissions
-- ... by the expected rate of readmissions, based on an average hospital with similar patients." Above 1, the
-- predicted rate is above the expected rate. Source: https://data.cms.gov/provider-data/dataset/9n3s-kdb3 and
-- https://data.cms.gov/provider-data/sites/default/files/data_dictionaries/hospital/HOSPITAL_Data_Dictionary.pdf
-- condition is the short code inside measure_name: READM-30-HF-HRRP becomes HF.
create or replace view cms_hrrp_history as
select
  p.facility_id,
  p.facility_name,
  p.state,
  p.measure_name,
  replace(replace(p.measure_name, 'READM-30-', ''), '-HRRP', '') as condition,
  try_cast(p.number_of_discharges as integer) as number_of_discharges,
  try_cast(p.number_of_readmissions as integer) as number_of_readmissions,
  p.number_of_readmissions as number_of_readmissions_text,
  try_cast(p.excess_readmission_ratio as double) as excess_readmission_ratio,
  try_cast(p.predicted_readmission_rate as double) as predicted_readmission_rate,
  try_cast(p.expected_readmission_rate as double) as expected_readmission_rate,
  p.footnote,
  cast(try_strptime(p.start_date, '%m/%d/%Y') as date) as start_date,
  cast(try_strptime(p.end_date, '%m/%d/%Y') as date) as end_date,
  p.cms_release_version,
  try_cast(p.cms_released as date) as cms_released,
  r.is_latest_release
from hrrp as p
inner join cms_release_history as r
  on r.table_name = 'hrrp'
  and r.release_version = p.cms_release_version
where not coalesce(p._fivetran_deleted, false);

create or replace view cms_hrrp as
select * exclude (is_latest_release)
from cms_hrrp_history
where is_latest_release;
