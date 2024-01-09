CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`customer_landing` (
  `customerName` string,
  `email` string,
  `phone` string,
  `birthDay` string,
  `registrationDate` bigint,
  `lastUpdateDate` bigint,
  `shareWithresearchAsOfDate` bigint,
  `shareWithPublicAsOfDate` bigint,
  `shareWithFriendAsOfDate` bigint,
  `serialNumber` string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-project-233718526883/customer_landing/'
TBLPROPERTIES ('classification' = 'json');