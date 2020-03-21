## Counting caught backouts





Chart of single job, and if it can be blamed for a regression

* **none** - Never run
* **fail** - always fails
* **pass** - always passed
* **inter** - intermittent

Deeper analysis:  For any test/manifest/job we can view its behaviour during three distinct periods

* **before** - how the test was behaving before the bad push
* **during** - how the test was behaving between the bad push and the execution of the backout
* **after** - how the test was behaving after backout was executed

For example, a test that was passing before the bad push, failing after the bad push, and then passing after the backout will be considered a good test to catch that bad push.  Here is the full truth table:



| before | during | after | blame | 
|--------|--------|-------|-------|
| none   |  none  | none  |       |
| fail   |  none  | none  |       |
| pass   |  none  | none  |       |
| inter  |  none  | none  |       |
| none   |  fail  | none  |       |
| fail   |  fail  | none  |       |
| pass   |  fail  | none  |   X   |
| inter  |  fail  | none  |       |
| none   |  pass  | none  |       |
| fail   |  pass  | none  |       |
| pass   |  pass  | none  |       |
| inter  |  pass  | none  |       |
| none   |  inter | none  |       |
| fail   |  inter | none  |       |
| pass   |  inter | none  |       |
| inter  |  inter | none  |       |





Simple case

|       | none | fail | pass | inter | 
|-------|------|------|------|-------|
| none  |      |      |      |       | 
| pass  |      |  `X` |      |  `X`  |
| inter |      |  `X` |      |       |
| fail  |      |      |      |       |


Where
* Rows: before the backed out
* Columns: After the backouted out
* `X` possible cause 





* before backout
* after backout and before backout_by
* after backout_by

