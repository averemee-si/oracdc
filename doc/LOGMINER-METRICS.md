#### Metrics for solutions.a2.cdc.oracle.OraCdcLogMinerConnector

**MBean:solutions.a2.oracdc:type=LogMiner-metrics,name=<Connector-Name>,database=$ORACLE_SID_<hostname>**

|Attribute Name                        |Type     |Description                                                                                       |
|:-------------------------------------|:--------|:-------------------------------------------------------------------------------------------------|
|TablesInProcessingCount               |int      |Number of tables in processing                                                                    |
|TablesInProcessing                    |String[] |Names of tables in processing                                                                     |
|PartitionsInProcessingCount           |int      |Number of partitions and subpartitions in processing                                              |
|Last100ProcessedArchivelogs           |String[] |Last 100 processed archivelogs                                                                    |
|LastProcessedArchivelog               |String   |Last processed archivelog                                                                         |
|LastProcessedScn                      |long     |Last processed SCN                                                                                |
|LastProcessedArchivelogTime           |String   |Time when the last archivelog was processed                                                       |
|TableOutOfScopeCount                  |int      |The number of tables from the journal that do not meet the a2.include criteria                    |
|NowProcessedArchivelogs               |String[] |Archivelogs currently being processed                                                             |
|CurrentFirstScn                       |long     |min(FIRST_CHANGE#) for archivelogs currently being processed                                      |
|CurrentNextScn                        |long     |max(NEXT_CHANGE#) for archivelogs currently being processed                                       |
|ProcessedArchivelogsCount             |int      |The number of processed archivelogs                                                               |
|ProcessedArchivelogsSizeGb            |float    |Size (gigabyte) of processed archivelogs                                                          |
|StartTime                             |String   |Connector start date and time (ISO format)                                                        |
|StartScn                              |long     |Connector start SCN                                                                               |
|ElapsedTimeMillis                     |long     |Elapsed time, milliseconds                                                                        |
|ElapsedTime                           |String   |Elapsed time, Days/Hours/Minutes/Seconds                                                          |
|TotalRecordsCount                     |long     |Total number of records processed                                                                 |
|RolledBackRecordsCount                |long     |The number of rolled back records processed                                                       |
|RolledBackTransactionsCount           |int      |The number of rolled back transactions processed                                                  |
|CommittedRecordsCount                 |long     |The number of committed records processed                                                         |
|CommittedTransactionsCount            |int      |The number of committed transactions processed                                                    |
|SentRecordsCount                      |long     |The number of records already sent to Kafka broker                                                |
|SentBatchesCount                      |int      |The number of Connector poll() call                                                               |
|ParseElapsedMillis                    |long     |Time spent for parsing V$LOGMNR_CONTENTS.SQL_REDO column, milliseconds                            |
|ParseElapsed                          |String   |Time spent for parsing V$LOGMNR_CONTENTS.SQL_REDO column, Days/Hours/Minutes/Seconds              |
|ParsePerSecond                        |int      |Average number of records parsed per second                                                       |
|RedoReadElapsedMillis                 |long     |Time spent for reading archivelogs (querying V$LOGMNR_CONTENTS), milliseconds                     |
|RedoReadElapsed                       |String   |Time spent for reading archivelogs (querying V$LOGMNR_CONTENTS), Days/Hours/Minutes/Seconds       |
|RedoReadMbPerSecond                   |float    |Average MB per second of archivelog reading                                                       |
|ActualLagSeconds                      |int      |Actual lag in seconds (SYSDATE-V$ARACHIVED_LOG.FIRST_TIME) for currently processing archived log  |
|ActualLagText                         |String   |Actual lag in for currently processing archived log, Days/Hours/Minutes/Seconds                   |
|DdlColumnsCount                       |int      |Number of successfully changed by DDL commands columns                                            |
|DdlElapsedMillis                      |long     |Time spent for processing DDL commands, milliseconds                                              |
|DdlElapsed                            |String   |Time spent for processing DDL commands, Days/Hours/Minutes/Seconds                                |
|MaxTransactionSizeBytes               |long     |Maximum transaction size i.e. number of bytes allocated for off-heap storage, bytes               |
|MaxTransactionSizeMiB                 |float    |Maximum transaction size i.e. number of bytes allocated for off-heap storage, MiB                 |
|MaxNumberOfTransInSendQueue           |int      |Maximum number of committed transactions in the send queue                                        | 
|CurrentNumberOfTransInSendQueue       |int      |Current number of committed transactions in the send queue                                        |
|MaxNumberOfTransInProcessingQueue     |int      |Maximum number of committed transactions in the processing queue                                  |
|CurrentNumberOfTransInProcessingQueue |int      |Current number of committed transactions in the processing queue                                  |
|NumBytesWrittenUsingChronicleQueue    |long     |Total number of bytes written to Chronicle Queue memory mapped files (off-heap storage)           |
|GiBWrittenUsingChronicleQueue         |float    |Total number of GiB written to Chronicle Queue memory mapped files (off-heap storage)             |