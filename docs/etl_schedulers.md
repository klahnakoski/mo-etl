## Counting caught backouts

* Never run
* run but failed
* run and passed
* run and intermittent

* before backout
* after backout and before backout_by
* after backout_by


Rows: before the rollback

|       | none | fail | pass | inter | 
|-------|------|------|------|-------|
| none  |      |      |      |       | 
| fail  |      |      |      |       |
| pass  |      |  X   |      |   X   |
| inter |      |  X   |      |       |