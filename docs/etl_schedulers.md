## Counting caught backouts

* Never run
* run but failed
* run and passed
* run and intermittent

* before backout
* after backout and before backout_by
* after backout_by



### Chart of single job, and if it can be blamed for a regression

* **none** - Never run
* **fail** - always fails
* **pass** - always passed
* **inter** - intermittent


* Rows: before the backed out
* Columns: After the backouted out
* `X` possible cause 

|       | none | fail | pass | inter | 
|-------|------|------|------|-------|
| none  |      |      |      |       | 
| fail  |      |      |      |       |
| pass  |      |  `X` |      |  `X`  |
| inter |      |  `X` |      |       |