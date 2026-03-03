# SQLHelper

Put any dict or json inside an sqlite database. It automatically creates and alters tables. Auto guesses types.
Async and synchronous SQLite helper library.


## Installation

```bash
pip install sqlhelper
```

## Usage

```python
from sqlhelper import SQLHelper

db = SQLHelper("mydb.db")
db.addobject("users", {"name": "Alice", "age": 30})
```
