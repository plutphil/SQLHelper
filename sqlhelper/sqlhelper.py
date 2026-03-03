import asyncio
import aiosqlite
import traceback

import aiosqlite

class SQLHelperAsync:
    def __init__(self, database="sqlhelper.db", db_type="sqlite", host=None, port=None, user=None, password=None, prefix=""):
        self.sqlconnection = None
        self.db_type = db_type.lower()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.PREFIX = prefix

    async def loaddb(self):
        if self.db_type == "sqlite":
            self.sqlconnection = await aiosqlite.connect(self.database)
        elif self.db_type == "postgresql":
            import asyncpg
            self.sqlconnection = await asyncpg.connect(
                host=self.host,
                port=self.port or 5432,
                user=self.user,
                password=self.password,
                database=self.database
            )
        elif self.db_type == "mysql":
            import aiomysql
            self.sqlconnection = await aiomysql.connect(
                host=self.host,
                port=self.port or 3306,
                user=self.user,
                password=self.password,
                db=self.database
            )
        elif self.db_type == "duckdb":
            # DuckDB is synchronous, so we wrap it in a thread for async usage
            import duckdb
            import anyio  # For async thread-safe execution for DuckDB
            self.sqlconnection = await anyio.to_thread.run_sync(lambda: duckdb.connect(database=self.database))
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    async def close(self):
        if self.sqlconnection:
            if self.db_type in ["sqlite", "postgresql", "mysql"]:
                await self.sqlconnection.commit()
                await self.sqlconnection.close()
            elif self.db_type == "duckdb":
                await anyio.to_thread.run_sync(self.sqlconnection.close)

    async def _get_existing_columns(self, table):
        cursor = await self.sqlconnection.cursor()
        await cursor.execute(f"PRAGMA table_info({self.PREFIX}{table});")
        return {row[1] for row in await cursor.fetchall()}

    async def addobject(self, obj: str, data: dict, addindex=True, adddate=True):
        cursor = await self.sqlconnection.cursor()
        # Create table if it doesn't exist
        lists = {}
        types = []
        for k, v in data.items():
            if isinstance(v, dict):
                v = await self.addobjifnotexist(k, v)
                t = "INT"
            elif isinstance(v, bool):
                t = "INT"
            elif isinstance(v, int):
                t = "INT"
            elif isinstance(v, float):
                t = "FLOAT"
            elif isinstance(v, bytes):
                t = "BLOB"
            elif isinstance(v, list):
                lists[k] = v
                continue
                #v = str(v)
                #t = "TEXT"
            else:
                t = "TEXT"
            types.append((k, t, v))

        existing_cols = await self._get_existing_columns(obj)
        if not existing_cols:
            sql_command = f"CREATE TABLE {self.PREFIX}{obj} ("
            if addindex:
                sql_command += "id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, "
            if adddate:
                sql_command += "datecreated TEXT,"
            sql_command += ", ".join([f"{k} {t}" for k, t, _ in types])
            # when object is empty remove comma
            if sql_command.endswith(","): 
                sql_command = sql_command[:-1]
                
            sql_command += ");"
            
            await cursor.execute(sql_command)
        else:
            for k, t, _ in types:
                if k not in existing_cols:
                    await cursor.execute(f"ALTER TABLE {self.PREFIX}{obj} ADD COLUMN {k} {t};")

        inserttypes = []
        insertqmarks = []
        values = []

        if adddate:
            inserttypes.append("datecreated")
            insertqmarks.append("datetime('now')")

        for k, t, v in types:
            inserttypes.append(k)
            insertqmarks.append("?")
            values.append(v)

        sql = f"INSERT INTO {self.PREFIX}{obj} ({', '.join(inserttypes)}) VALUES ({', '.join(insertqmarks)});"
        await cursor.execute(sql, tuple(values))
        await self.sqlconnection.commit()
        
        newrowid = cursor.lastrowid
        for k,v in lists.items():
            for e in v:
                if isinstance(e,dict):
                    e[obj+"_id"] = newrowid
                    await self.addobjifnotexist(obj+"_"+k,e,False,False)
                else:
                    await self.addobjifnotexist(obj+"_"+k,{k:e,obj+"_id":newrowid},False,False)
        
        return newrowid

    async def addobjifnotexist(self, objname: str, data: dict, addindex=True, adddate=True):
        obj = await self.sqlfindmult(objname, data)
        if obj is None:
            return await self.addobject(objname, data, addindex, adddate)
        return obj[0]

    async def sqlfind(self, table, col, val):
        cursor = await self.sqlconnection.cursor()
        try:
            await cursor.execute(f"SELECT * FROM {self.PREFIX}{table} WHERE {col} = ?", (val,))
            return await cursor.fetchone()
        except Exception:
            return None

    async def sqlfindmult(self, table, dic: dict):
        cursor = await self.sqlconnection.cursor()
        try:
            wherestuff = " = ? AND ".join(list(dic.keys())) + " = ? "
            await cursor.execute(f"SELECT * FROM {self.PREFIX}{table} WHERE {wherestuff}", tuple(dic.values()))
            return await cursor.fetchone()
        except Exception:
            return None

    async def sqlgetall(self, table, field="*"):
        try:
            cursor = await self.sqlconnection.cursor()
            await cursor.execute(f"SELECT {field} FROM {self.PREFIX}{table}")
            return await cursor.fetchall()
        except Exception as ex:
            traceback.print_exception(type(ex), ex, ex.__traceback__)
            return None

    async def getorcreateindex(self, table, col, val):
        res = await self.sqlfind(table, col, val)
        if res is None:
            await self.addobject(table, {col: val}, True, False)
            res = await self.sqlfind(table, col, val)
        return res[0]

        
    async def runsql(self, sql: str, params: tuple = ()):
        """Run raw SQL with optional parameters and return cursor.fetchall() if available."""
        cursor = await self.sqlconnection.cursor()
        try:
            await cursor.execute(sql, params)
            if sql.strip().upper().startswith("SELECT"):
                return await cursor.fetchall()
            else:
                await self.sqlconnection.commit()
                return cursor.lastrowid
        except Exception as ex:
            traceback.print_exception(type(ex), ex, ex.__traceback__)
            return None


# ----------- Non-async wrapper -----------
class SQLHelper:
    def __init__(self, path="sqlhelper.db", prefix=""):
        self._async = SQLHelperAsync(path, prefix=prefix)
        asyncio.run(self._async.loaddb())

    def addobject(self, *args, **kwargs):
        return asyncio.run(self._async.addobject(*args, **kwargs))

    def addobjifnotexist(self, *args, **kwargs):
        return asyncio.run(self._async.addobjifnotexist(*args, **kwargs))

    def sqlfind(self, *args, **kwargs):
        return asyncio.run(self._async.sqlfind(*args, **kwargs))

    def sqlfindmult(self, *args, **kwargs):
        return asyncio.run(self._async.sqlfindmult(*args, **kwargs))

    def sqlgetall(self, *args, **kwargs):
        return asyncio.run(self._async.sqlgetall(*args, **kwargs))

    def getorcreateindex(self, *args, **kwargs):
        return asyncio.run(self._async.getorcreateindex(*args, **kwargs))
    
    def runsql(self, *args, **kwargs):
        return asyncio.run(self._async.runsql(*args, **kwargs))

    def close(self):
        return asyncio.run(self._async.close())


# ----------- Test function -----------
def test_sqlhelper():
    db = SQLHelper("test.db", prefix="t_")

    print("Adding object...")
    rowid = db.addobject("users", {"name": "Alice", "age": 30})
    print("Row ID:", rowid)

    print("Finding object...")
    row = db.sqlfind("users", "name", "Alice")
    print("Found:", row)

    print("Adding duplicate (should reuse)...")
    existing_id = db.addobjifnotexist("users", {"name": "Alice", "age": 30})
    print("Existing ID:", existing_id)

    print("All rows:")
    rows = db.sqlgetall("users")
    for r in rows:
        print(r)
    
    print("All rows after raw insert:")
    print(db.runsql("SELECT * FROM t_users"))
    
    db.close()

if __name__ == "__main__":
    test_sqlhelper()
