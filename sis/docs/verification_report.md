# Pipeline verification

| Stage | Records | Explanation |
|---|---:|---|
| 1. Collected from API | 3760 | raw_data.csv, saved before any cleaning |
| 2. Published to Kafka | 3763 | collected + 3 deliberately invalid test records |
| 3. Consumed from Kafka | 3763 | must equal published (stop reason: all expected messages consumed) |
| 4. Valid (Pandera) | 3760 | consumed - invalid |
| 5. Invalid (Pandera) | 3 | 3 of 3 test records detected, 0 real invalid |
| 6. After cleaning | 3760 | valid - 0 duplicates - 0 filtered |
| 7. Stored in SQLite | 3760 | must equal cleaned; second load added 0 rows |

**ALL CHECKS PASSED**