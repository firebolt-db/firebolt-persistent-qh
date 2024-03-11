# Persistent Query History in Firebolt
Query history, a chronological record of past queries executed in Firebolt, serves as a valuable resource for users to review, analyze, and learn from their interactions with the database. Recognizing its significance in troubleshooting, optimizing performance, and understanding historical data access patterns, Firebolt introduces an enhanced Query History Persistence feature.

> Note: currently, this feature is exposed as a beta feature

## Challenges Addressed
Currently, query history in Firebolt is lost on engine restart or after two weeks, limiting users' ability to troubleshoot, optimize, or understand historical query and data access patterns over time. In response to user feedback, we've developed a solution to overcome these limitations.

**New Key Capabilities:**
* Extended Storage Options: Users can now store query history as Parquet files in their specified S3 bucket, providing flexibility in determining the duration of query history persistence. Users delete parquet files, that system wrote, from  S3 bucket when they consider these files as not needed anymore.
* Preventing Data Loss: In contrast to the current scenario where query history is erased during engine restarts or when users stop the engine, S3 query history logs will persist, guaranteeing uninterrupted access to crucial historical data.

We're releasing an alpha release of our persistence query history capability. Dive in, explore the extended data retention, and leverage the advanced query analysis tools. Your valuable feedback is pivotal in refining this feature for its official launch! Join us on this alpha journey to shape the future of Firebolt.

## Setup
As this feature is released as a beta version, users are requested to perform the following steps for activation:

* **Account Information:** Provide Firebolt Account ID.
* **Configuration:** Specify the S3 bucket and folder for storing query history parquet files from Firebolt Account ID.
* **Permissions:** Ask for a Firebolt owned AWS IAM role and AWS account. Grant the provided Firebolt AWS IAM role required write, list, and read permissions to the designated S3 folder
* **Pattern:** `arn:aws:iam::<Firebolt provided AWS id>:role/FireboltData_<firebolt-account-id>`
    Ensure that the S3 bucket policy is configured accurately, following the provided example below for reference. 
```{
 "Version": "2012-10-17",
 "Statement": [
   {
      "Effect": "Allow",
      "Principal": {
        "AWS":  [
                "arn:aws:iam::<Firebolt provided AWS id>:role/FireboltData_<firebolt-account-id>"
                ]
            },
      "Action": [
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:PutObjectAcl",
                "s3:Get*",
                "s3:List*"
            ],
      "Resource": [ "arn:aws:s3:::<s3-bucket>/*/account_id=<firebolt-account-id>/*"
            ]
        }
    ]
}
```

> Note: Make sure you replace `<firebolt-account-id>` and `<s3-bucket>` with your own firebolt account id and s3 bucket. Also please ask Firebolt to provide a '<**firebolt provide AWS id**>'

Upon receipt of this information, we will enable this capability on our end for the specified account.

## Using Persistent Query History
We recommend connecting Firebolt to the query history parquet files stored in S3. You will then be able to analyze your query history using SQL, or connect your preferred BI/reporting tools directly to your query history tables. 

The following steps outline how to create your query history table in Firebolt, and include links to sample DDL code:
1. [Create an external table](/query_history_ddl/create_external_table.sql)
2. [Create a fact table](/query_history_ddl/create_fact_table.sql)
3. [Insert data into fact table](/query_history_ddl/create_fact_table.sql)
4. [(Optional) Configure aggregating indexes on your fact table](/query_history_ddl/aggregating_indexes.sql)

## Example Queries
This repository contains a set of example queries that you can use to analyze your persistent query history table. The following table provides links to each sample query and a brief description of how the query can be used. You can use these queries as the basis for your own Firebolt monitoring solution, or create your own queries based on your specific questions.

| Query Name | Description |
| ---------- | ----------- |
| [query_concurrency.sql](/example_queries/query_concurrency.sql) | Use this query to understand the approximate load on your Firebolt engine at different time periods. Output columns include the average number of concurrent queries per minute per hour, the average RAM consumed per query per minute per hour, and the average query duration per minute per hour. |
| [query_drilldown.sql](/example_queries/query_drilldown.sql) | Use this query to understand the duration and resource consumption of individual query patterns at different time periods. This allows you to identify queries that could benefit from performance optimization. |

Refer to the [Firebolt documentation](https://docs.firebolt.io/godocs/sql_reference/information-schema/query-history-view.html) for a detailed explanation of the columns available in `information_schema.query_history`. 
