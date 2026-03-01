DROP TABLE IF EXISTS stg_lending_club_raw;

CREATE TABLE stg_lending_club_raw (
    id TEXT,
    member_id TEXT,
    loan_amnt TEXT,
    funded_amnt TEXT,
    funded_amnt_inv TEXT,
    term TEXT,
    int_rate TEXT,
    installment TEXT,
    grade TEXT,
    sub_grade TEXT,
    emp_title TEXT,
    emp_length TEXT,
    home_ownership TEXT,
    annual_inc TEXT,
    verification_status TEXT,
    issue_d TEXT,
    loan_status TEXT,
    pymnt_plan TEXT,
    url TEXT,
    "desc" TEXT,
    purpose TEXT,
    title TEXT,
    zip_code TEXT,
    addr_state TEXT,
    dti TEXT,
    delinq_2yrs TEXT,
    earliest_cr_line TEXT,
    fico_range_low TEXT,
    fico_range_high TEXT,
    inq_last_6mths TEXT,
    mths_since_last_delinq TEXT,
    mths_since_last_record TEXT,
    open_acc TEXT,
    pub_rec TEXT,
    revol_bal TEXT,
    revol_util TEXT,
    total_acc TEXT,
    initial_list_status TEXT,
    out_prncp TEXT,
    out_prncp_inv TEXT,
    total_pymnt TEXT,
    total_pymnt_inv TEXT,
    total_rec_prncp TEXT,
    total_rec_int TEXT,
    total_rec_late_fee TEXT,
    recoveries TEXT,
    collection_recovery_fee TEXT,
    last_pymnt_d TEXT,
    last_pymnt_amnt TEXT,
    next_pymnt_d TEXT,
    last_credit_pull_d TEXT,
    last_fico_range_high TEXT,
    last_fico_range_low TEXT,
    collections_12_mths_ex_med TEXT,
    mths_since_last_major_derog TEXT,
    policy_code TEXT,
    application_type TEXT,
    annual_inc_joint TEXT,
    dti_joint TEXT,
    verification_status_joint TEXT,
    acc_now_delinq TEXT,
    tot_coll_amt TEXT,
    tot_cur_bal TEXT,
    open_acc_6m TEXT,
    open_act_il TEXT,
    open_il_12m TEXT,
    open_il_24m TEXT,
    mths_since_rcnt_il TEXT,
    total_bal_il TEXT,
    il_util TEXT,
    open_rv_12m TEXT,
    open_rv_24m TEXT,
    max_bal_bc TEXT,
    all_util TEXT,
    total_rev_hi_lim TEXT,
    inq_fi TEXT,
    total_cu_tl TEXT,
    inq_last_12m TEXT,
    acc_open_past_24mths TEXT,
    avg_cur_bal TEXT,
    bc_open_to_buy TEXT,
    bc_util TEXT,
    chargeoff_within_12_mths TEXT,
    delinq_amnt TEXT,
    mo_sin_old_il_acct TEXT,
    mo_sin_old_rev_tl_op TEXT,
    mo_sin_rcnt_rev_tl_op TEXT,
    mo_sin_rcnt_tl TEXT,
    mort_acc TEXT,
    mths_since_recent_bc TEXT,
    mths_since_recent_bc_dlq TEXT,
    mths_since_recent_inq TEXT,
    mths_since_recent_revol_delinq TEXT,
    num_accts_ever_120_pd TEXT,
    num_actv_bc_tl TEXT,
    num_actv_rev_tl TEXT,
    num_bc_sats TEXT,
    num_bc_tl TEXT,
    num_il_tl TEXT,
    num_op_rev_tl TEXT,
    num_rev_accts TEXT,
    num_rev_tl_bal_gt_0 TEXT,
    num_sats TEXT,
    num_tl_120dpd_2m TEXT,
    num_tl_30dpd TEXT,
    num_tl_90g_dpd_24m TEXT,
    num_tl_op_past_12m TEXT,
    pct_tl_nvr_dlq TEXT,
    percent_bc_gt_75 TEXT,
    pub_rec_bankruptcies TEXT,
    tax_liens TEXT,
    tot_hi_cred_lim TEXT,
    total_bal_ex_mort TEXT,
    total_bc_limit TEXT,
    total_il_high_credit_limit TEXT,
    revol_bal_joint TEXT,
    sec_app_fico_range_low TEXT,
    sec_app_fico_range_high TEXT,
    sec_app_earliest_cr_line TEXT,
    sec_app_inq_last_6mths TEXT,
    sec_app_mort_acc TEXT,
    sec_app_open_acc TEXT,
    sec_app_revol_util TEXT,
    sec_app_open_act_il TEXT,
    sec_app_num_rev_accts TEXT,
    sec_app_chargeoff_within_12_mths TEXT,
    sec_app_collections_12_mths_ex_med TEXT,
    sec_app_mths_since_last_major_derog TEXT,
    hardship_flag TEXT,
    hardship_type TEXT,
    hardship_reason TEXT,
    hardship_status TEXT,
    deferral_term TEXT,
    hardship_amount TEXT,
    hardship_start_date TEXT,
    hardship_end_date TEXT,
    payment_plan_start_date TEXT,
    hardship_length TEXT,
    hardship_dpd TEXT,
    hardship_loan_status TEXT,
    orig_projected_additional_accrued_interest TEXT,
    hardship_payoff_balance_amount TEXT,
    hardship_last_payment_amount TEXT,
    disbursement_method TEXT,
    debt_settlement_flag TEXT,
    debt_settlement_flag_date TEXT,
    settlement_status TEXT,
    settlement_date TEXT,
    settlement_amount TEXT,
    settlement_percentage TEXT,
    settlement_term TEXT
);



COPY stg_lending_club_raw
FROM 'C:/data/loan.csv'
DELIMITER ','
CSV HEADER
QUOTE '"'
NULL '';



CREATE TABLE fact_loans AS
SELECT
    id::BIGINT AS loan_id,

    TO_DATE(issue_d, 'Mon-YYYY') AS issue_date,

    loan_status,

    NULLIF(loan_amnt, '')::NUMERIC AS loan_amount,
    NULLIF(funded_amnt, '')::NUMERIC AS funded_amount,

    term,

    NULLIF(int_rate, '')::NUMERIC AS interest_rate,

    grade,
    sub_grade,

    NULLIF(fico_range_low, '')::NUMERIC AS fico_low,
    NULLIF(fico_range_high, '')::NUMERIC AS fico_high,

    NULLIF(annual_inc, '')::NUMERIC AS annual_income,

    emp_length,
    home_ownership,

    addr_state,

    NULLIF(total_pymnt, '')::NUMERIC AS total_payment,
    NULLIF(recoveries, '')::NUMERIC AS recoveries

FROM stg_lending_club_raw
WHERE issue_d IS NOT NULL;




CREATE TABLE dim_time (
    date_key INT PRIMARY KEY,
    calendar_date DATE,
    year INT,
    quarter TEXT,
    month INT,
    month_name TEXT,
    year_month TEXT,
    is_year_end BOOLEAN
);

INSERT INTO dim_time
SELECT
    TO_CHAR(d, 'YYYYMM')::INT            AS date_key,
    d                                   AS calendar_date,
    EXTRACT(YEAR FROM d)::INT            AS year,
    'Q' || EXTRACT(QUARTER FROM d)::INT  AS quarter,
    EXTRACT(MONTH FROM d)::INT           AS month,
    TO_CHAR(d, 'Mon')                    AS month_name,
    TO_CHAR(d, 'YYYY-MM')                AS year_month,
    CASE WHEN EXTRACT(MONTH FROM d) = 12 THEN TRUE ELSE FALSE END AS is_year_end
FROM generate_series(
    (SELECT MIN(issue_date) FROM fact_loans),
    (SELECT MAX(issue_date) FROM fact_loans),
    INTERVAL '1 month'
) d;





CREATE VIEW vw_portfolio_kpis AS
SELECT
    COUNT(*) AS total_loans,

    SUM(loan_amount) AS total_exposure,

    SUM(
        CASE
            WHEN loan_status IN ('Charged Off', 'Default')
            THEN loan_amount
            ELSE 0
        END
    ) AS defaulted_exposure,

    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE loan_status IN ('Charged Off', 'Default')
        ) / COUNT(*),
        2
    ) AS default_rate_pct

FROM fact_loans;





CREATE VIEW vw_monthly_trends AS
SELECT
    d.year_month,

    COUNT(f.loan_id) AS total_loans,

    SUM(f.loan_amount) AS total_exposure,

    SUM(
        CASE
            WHEN f.loan_status IN ('Charged Off', 'Default')
            THEN f.loan_amount
            ELSE 0
        END
    ) AS defaulted_exposure,

    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE f.loan_status IN ('Charged Off', 'Default')
        ) / COUNT(*),
        2
    ) AS default_rate_pct

FROM fact_loans f
JOIN dim_time d
  ON f.issue_date = d.calendar_date
GROUP BY d.year_month
ORDER BY d.year_month;




DROP VIEW IF EXISTS vw_fico_risk;

CREATE VIEW vw_fico_risk AS
SELECT
    CASE
        WHEN fico_low < 580 THEN 'Poor'
        WHEN fico_low BETWEEN 580 AND 669 THEN 'Fair'
        WHEN fico_low BETWEEN 670 AND 739 THEN 'Good'
        WHEN fico_low BETWEEN 740 AND 799 THEN 'Very Good'
        ELSE 'Excellent'
    END AS fico_band,

    COUNT(*) AS total_loans,
    SUM(loan_amount) AS exposure,

    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE loan_status IN ('Charged Off', 'Default')
        ) / COUNT(*),
        2
    ) AS default_rate_pct

FROM fact_loans
GROUP BY fico_band
ORDER BY default_rate_pct DESC;





DROP VIEW IF EXISTS vw_state_risk;

CREATE VIEW vw_state_risk AS
SELECT
    addr_state,

    COUNT(*) AS total_loans,
    SUM(loan_amount) AS exposure,

    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE loan_status IN ('Charged Off', 'Default')
        ) / COUNT(*),
        2
    ) AS default_rate_pct

FROM fact_loans
GROUP BY addr_state
ORDER BY exposure DESC;