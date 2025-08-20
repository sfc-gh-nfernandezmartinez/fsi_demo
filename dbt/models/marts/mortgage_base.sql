{{ config(materialized='table', tags=['ml_base']) }}

WITH src AS (
  SELECT
    LOAN_RECORD_ID,
    LOAN_ID,
    WEEK_START_DATE,
    WEEK,
    TS,
    -- normalize loan type
    UPPER(TRIM(
      REPLACE(
        REPLACE(
          REPLACE(LOAN_TYPE_NAME,
            'FSA/RHS-guaranteed', 'FSA_RHS'),
            'FHA-insured', 'FHA'),
            'VA-guaranteed', 'VA'
      )
    )) AS LOAN_TYPE_NAME,

    -- normalize loan purpose
    UPPER(TRIM(
      REPLACE(
        REPLACE(
          REPLACE(LOAN_PURPOSE_NAME,
            '-', '_'),
            ' ', '_'),
            '.', '_'
      )
    )) AS LOAN_PURPOSE_NAME,

    -- normalize county (remove -, _, .)
    UPPER(TRIM(TRANSLATE(COUNTY_NAME, '-_.', ''))) AS COUNTY_NAME,

    APPLICANT_INCOME_000S,
    LOAN_AMOUNT_000S,
    MORTGAGERESPONSE,
    CREATED_AT
  FROM {{ source('raw_data', 'mortgage_table') }}
  WHERE LOAN_RECORD_ID IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY LOAN_RECORD_ID ORDER BY CREATED_AT DESC) = 1
)

SELECT * FROM src;

