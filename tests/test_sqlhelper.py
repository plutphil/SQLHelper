import unittest
import os
from sqlhelper import SQLHelper

class TestSQLHelper(unittest.TestCase):

    def setUp(self):
        self.db = SQLHelper(":memory:")

    def tearDown(self):
        self.db.close()

    # -------------------------------------------------------------------------
    # addobject
    # -------------------------------------------------------------------------

    def test_add_and_find(self):
        rowid = self.db.addobject("users", {"name": "Alice", "age": 30})
        row = self.db.sqlfind("users", "name", "Alice")
        self.assertIsNotNone(row)

    def test_addobject_returns_rowid(self):
        rowid = self.db.addobject("items", {"label": "Widget"})
        self.assertIsInstance(rowid, int)
        self.assertGreater(rowid, 0)

    def test_addobject_autoincrement(self):
        id1 = self.db.addobject("items", {"label": "A"})
        id2 = self.db.addobject("items", {"label": "B"})
        self.assertGreater(id2, id1)

    def test_addobject_multiple_rows(self):
        self.db.addobject("products", {"name": "Foo", "price": 9.99})
        self.db.addobject("products", {"name": "Bar", "price": 4.99})
        rows = self.db.sqlgetall("products")
        self.assertEqual(len(rows), 2)

    def test_addobject_with_bool(self):
        self.db.addobject("flags", {"active": True, "deleted": False})
        row = self.db.sqlfind("flags", "active", 1)
        self.assertIsNotNone(row)

    def test_addobject_with_float(self):
        self.db.addobject("measurements", {"value": 3.14})
        row = self.db.sqlfind("measurements", "value", 3.14)
        self.assertIsNotNone(row)

    def test_addobject_with_bytes(self):
        self.db.addobject("blobs", {"data": b"hello bytes"})
        row = self.db.sqlfind("blobs", "data", b"hello bytes")
        self.assertIsNotNone(row)

    def test_addobject_with_list(self):
        self.db.addobject("lists", {"items": [1, 2, 3]})
        row = self.db.sqlfind("lists_items", "items", 1)
        self.assertIsNotNone(row)
        row = self.db.sqlfind("lists_items", "items", 2)
        self.assertIsNotNone(row)
        row = self.db.sqlfind("lists_items", "items", 3)
        self.assertIsNotNone(row)

    def test_addobject_adds_datecreated_by_default(self):
        self.db.addobject("events", {"title": "Launch"})
        row = self.db.sqlfind("events", "title", "Launch")
        # row columns: id, datecreated, title
        self.assertIsNotNone(row[1])  # datecreated should be populated

    def test_addobject_no_date(self):
        self.db.addobject("nodateitems", {"name": "X"}, adddate=False)
        row = self.db.sqlfind("nodateitems", "name", "X")
        self.assertIsNotNone(row)

    def test_addobject_no_index(self):
        self.db.addobject("noindex", {"name": "Y"}, addindex=False)
        row = self.db.sqlfind("noindex", "name", "Y")
        self.assertIsNotNone(row)

    def test_addobject_adds_new_column_to_existing_table(self):
        self.db.addobject("dynamic", {"col_a": "hello"})
        self.db.addobject("dynamic", {"col_a": "world", "col_b": 42})
        row = self.db.sqlfind("dynamic", "col_b", 42)
        self.assertIsNotNone(row)

    def test_addobject_nested_dict_creates_foreign_row(self):
        # A nested dict should store the nested object and return its id as INT
        rowid = self.db.addobject("orders", {"ref": {"code": "ABC"}})
        self.assertIsInstance(rowid, int)

    # -------------------------------------------------------------------------
    # sqlfind
    # -------------------------------------------------------------------------

    def test_sqlfind_returns_correct_row(self):
        self.db.addobject("users", {"name": "Bob", "age": 25})
        row = self.db.sqlfind("users", "name", "Bob")
        self.assertIsNotNone(row)
        self.assertIn("Bob", row)

    def test_sqlfind_returns_none_for_missing(self):
        self.db.addobject("users", {"name": "Alice"})
        row = self.db.sqlfind("users", "name", "Ghost")
        self.assertIsNone(row)

    def test_sqlfind_returns_none_on_bad_table(self):
        row = self.db.sqlfind("nonexistent_table", "col", "val")
        self.assertIsNone(row)

    def test_sqlfind_returns_first_match_only(self):
        self.db.addobject("users", {"name": "Twin"})
        self.db.addobject("users", {"name": "Twin"})
        row = self.db.sqlfind("users", "name", "Twin")
        self.assertIsNotNone(row)
        # fetchone — should be a single tuple, not a list
        self.assertNotIsInstance(row, list)

    # -------------------------------------------------------------------------
    # sqlfindmult
    # -------------------------------------------------------------------------

    def test_sqlfindmult_finds_matching_row(self):
        self.db.addobject("users", {"name": "Carol", "age": 40})
        row = self.db.sqlfindmult("users", {"name": "Carol", "age": 40})
        self.assertIsNotNone(row)

    def test_sqlfindmult_returns_none_on_partial_mismatch(self):
        self.db.addobject("users", {"name": "Dave", "age": 30})
        row = self.db.sqlfindmult("users", {"name": "Dave", "age": 99})
        self.assertIsNone(row)

    def test_sqlfindmult_returns_none_on_bad_table(self):
        row = self.db.sqlfindmult("no_such_table", {"x": 1})
        self.assertIsNone(row)

    # -------------------------------------------------------------------------
    # sqlgetall
    # -------------------------------------------------------------------------

    def test_sqlgetall_returns_all_rows(self):
        for name in ["A", "B", "C"]:
            self.db.addobject("letters", {"name": name})
        rows = self.db.sqlgetall("letters")
        self.assertEqual(len(rows), 3)

    def test_sqlgetall_returns_empty_list_for_empty_table(self):
        self.db.addobject("empty_table", {"placeholder": "x"})
        # Wipe contents via raw SQL, keep table
        self.db.runsql("DELETE FROM empty_table")
        rows = self.db.sqlgetall("empty_table")
        self.assertEqual(rows, [])

    def test_sqlgetall_returns_none_for_missing_table(self):
        rows = self.db.sqlgetall("does_not_exist")
        self.assertIsNone(rows)

    def test_sqlgetall_specific_field(self):
        self.db.addobject("users", {"name": "Eve", "age": 22})
        rows = self.db.sqlgetall("users", field="name")
        self.assertIsNotNone(rows)
        self.assertTrue(all(len(r) == 1 for r in rows))

    # -------------------------------------------------------------------------
    # addobjifnotexist
    # -------------------------------------------------------------------------

    def test_addobjifnotexist_creates_if_missing(self):
        idx = self.db.addobjifnotexist("tags", {"label": "python"})
        self.assertIsNotNone(idx)

    def test_addobjifnotexist_reuses_existing(self):
        id1 = self.db.addobjifnotexist("tags", {"label": "reuse"})
        id2 = self.db.addobjifnotexist("tags", {"label": "reuse"})
        self.assertEqual(id1, id2)

    def test_addobjifnotexist_does_not_duplicate(self):
        self.db.addobjifnotexist("tags", {"label": "unique"})
        self.db.addobjifnotexist("tags", {"label": "unique"})
        rows = self.db.sqlgetall("tags")
        count = sum(1 for r in rows if "unique" in r)
        self.assertEqual(count, 1)

    # -------------------------------------------------------------------------
    # getorcreateindex
    # -------------------------------------------------------------------------

    def test_getorcreateindex_creates_new(self):
        idx = self.db.getorcreateindex("categories", "name", "Fiction")
        self.assertIsInstance(idx, int)

    def test_getorcreateindex_returns_same_id(self):
        idx1 = self.db.getorcreateindex("categories", "name", "Science")
        idx2 = self.db.getorcreateindex("categories", "name", "Science")
        self.assertEqual(idx1, idx2)

    def test_getorcreateindex_different_values_different_ids(self):
        idx1 = self.db.getorcreateindex("categories", "name", "Horror")
        idx2 = self.db.getorcreateindex("categories", "name", "Romance")
        self.assertNotEqual(idx1, idx2)

    # -------------------------------------------------------------------------
    # runsql
    # -------------------------------------------------------------------------

    def test_runsql_select(self):
        self.db.addobject("users", {"name": "Frank"})
        rows = self.db.runsql("SELECT * FROM users")
        self.assertIsNotNone(rows)
        self.assertGreater(len(rows), 0)

    def test_runsql_select_with_params(self):
        self.db.addobject("users", {"name": "Grace"})
        rows = self.db.runsql("SELECT * FROM users WHERE name = ?", ("Grace",))
        self.assertIsNotNone(rows)
        self.assertEqual(len(rows), 1)

    def test_runsql_insert_returns_rowid(self):
        self.db.addobject("users", {"name": "Placeholder"})  # ensure table exists
        rowid = self.db.runsql(
            "INSERT INTO users (name) VALUES (?)", ("Harry",)
        )
        self.assertIsInstance(rowid, int)

    def test_runsql_delete(self):
        self.db.addobject("users", {"name": "ToDelete"})
        self.db.runsql("DELETE FROM users WHERE name = ?", ("ToDelete",))
        row = self.db.sqlfind("users", "name", "ToDelete")
        self.assertIsNone(row)

    def test_runsql_bad_sql_returns_none(self):
        result = self.db.runsql("THIS IS NOT VALID SQL !!!!")
        self.assertIsNone(result)

    # -------------------------------------------------------------------------
    # prefix support
    # -------------------------------------------------------------------------

    def test_prefix_isolates_tables(self):
        db_a = SQLHelper(":memory:", prefix="a_")
        db_b = SQLHelper(":memory:", prefix="b_")
        db_a.addobject("items", {"name": "alpha"})
        db_b.addobject("items", {"name": "beta"})
        rows_a = db_a.sqlgetall("items")
        rows_b = db_b.sqlgetall("items")
        self.assertEqual(len(rows_a), 1)
        self.assertEqual(len(rows_b), 1)
        db_a.close()
        db_b.close()

    # -------------------------------------------------------------------------
    # persistence (file-based db)
    # -------------------------------------------------------------------------

    def test_file_db_persists_across_connections(self):
        path = "/tmp/test_persist.db"
        try:
            db1 = SQLHelper(path)
            db1.addobject("persist_test", {"value": "hello"})
            db1.close()

            db2 = SQLHelper(path)
            row = db2.sqlfind("persist_test", "value", "hello")
            db2.close()

            self.assertIsNotNone(row)
        finally:
            if os.path.exists(path):
                os.remove(path)

import unittest
import asyncio

class TestSQLHelperLists(unittest.TestCase):

    def setUp(self):
        import aiosqlite
        from sqlhelper import SQLHelperAsync  # replace with your actual import
        self.loop = asyncio.get_event_loop()
        self.db = SQLHelperAsync(":memory:")
        self.loop.run_until_complete(self.db.loaddb())  # assuming you have a connect method

    def tearDown(self):
        self.loop.run_until_complete(self.db.sqlconnection.close())


    # Test addobject with list handling
    def test_addobject_with_list(self):
        async def test():
            data = {
                "title": "Parent Object",
                "items": [
                    {"subname": "A"},
                    {"subname": "B"}
                ]
            }
            row_id = await self.db.addobject("parent_table", data)
            self.assertIsInstance(row_id, int)

            # check that addobjifnotexist was called for each list item
            # here we mock addobjifnotexist to record calls
            called_items = []

            async def mock_addobjifnotexist(name, data, addindex=True, adddate=True):
                called_items.append((name, data))
                return 100  # fake ID

            self.db.addobjifnotexist = mock_addobjifnotexist
            await self.db.addobject("parent_table", data)

            self.assertEqual(len(called_items), 2)
            self.assertEqual(called_items[0][1]["subname"], "A")
            self.assertEqual(called_items[1][1]["subname"], "B")

        self.loop.run_until_complete(test())
    
    async def _fetch_all_rows(self, table):
        """Return every row from *table* as a list of sqlite3.Row-like tuples."""
        cursor = await self.db.sqlconnection.cursor()
        await cursor.execute(f"SELECT * FROM {table}")
        return await cursor.fetchall()

    async def _column_names(self, table):
        """Return the column names for *table*."""
        cursor = await self.db.sqlconnection.cursor()
        await cursor.execute(f"PRAGMA table_info({table})")
        return [row[1] for row in await cursor.fetchall()]

    # ── tests ─────────────────────────────────────────────────────────────────

    def test_addobject_with_list_returns_valid_id(self):
        """addobject returns an integer row-id even when the data contains lists."""
        async def run():
            data = {
                "title": "Parent Object",
                "items": [{"subname": "A"}, {"subname": "B"}],
            }
            row_id = await self.db.addobject("parent", data)
            self.assertIsInstance(row_id, int)
            self.assertGreater(row_id, 0)

        self.loop.run_until_complete(run())

    def test_list_items_stored_in_child_table(self):
        """Each element of a list field must appear as a row in the child table
        (named  <parent>_<field>) with the correct scalar values."""
        async def run():
            data = {
                "title": "Parent Object",
                "items": [{"subname": "A"}, {"subname": "B"}],
            }
            await self.db.addobject("parent", data)

            rows = await self._fetch_all_rows("parent_items")
            self.assertEqual(len(rows), 2, "Expected exactly 2 child rows")

            cols = await self._column_names("parent_items")
            subname_idx = cols.index("subname")
            subnames = {row[subname_idx] for row in rows}
            self.assertEqual(subnames, {"A", "B"})

        self.loop.run_until_complete(run())

    def test_list_items_carry_parent_foreign_key(self):
        """Child rows must have a  <parent>_id  column pointing back to the
        parent row that was just inserted."""
        async def run():
            data = {
                "title": "Parent Object",
                "items": [{"subname": "A"}, {"subname": "B"}],
            }
            parent_id = await self.db.addobject("parent", data)

            rows = await self._fetch_all_rows("parent_items")
            cols = await self._column_names("parent_items")

            self.assertIn(
                "parent_id", cols,
                "Child table must contain a 'parent_id' foreign-key column",
            )
            fk_idx = cols.index("parent_id")
            for row in rows:
                self.assertEqual(
                    row[fk_idx],
                    parent_id,
                    f"Child row {row} does not reference parent id {parent_id}",
                )

        self.loop.run_until_complete(run())

    def test_list_items_retrievable_by_foreign_key(self):
        """sqlgetall + manual filter must return only the children that belong
        to a specific parent when multiple parents exist."""
        async def run():
            id_one = await self.db.addobject(
                "parent", {"title": "First", "items": [{"subname": "X"}, {"subname": "Y"}]}
            )
            id_two = await self.db.addobject(
                "parent", {"title": "Second", "items": [{"subname": "Z"}]}
            )

            all_children = await self.db.sqlgetall("parent_items")
            cols = await self._column_names("parent_items")
            fk_idx = cols.index("parent_id")
            subname_idx = cols.index("subname")

            children_of_one = [r for r in all_children if r[fk_idx] == id_one]
            children_of_two = [r for r in all_children if r[fk_idx] == id_two]

            self.assertEqual(len(children_of_one), 2)
            self.assertEqual(len(children_of_two), 1)
            self.assertEqual(
                {r[subname_idx] for r in children_of_one}, {"X", "Y"}
            )
            self.assertEqual(
                {r[subname_idx] for r in children_of_two}, {"Z"}
            )

        self.loop.run_until_complete(run())

    def test_duplicate_list_items_are_deduplicated(self):
        """addobjifnotexist must not insert a child row that already exists
        with the same data (including the same parent_id)."""
        async def run():
            data = {
                "title": "Parent",
                "items": [{"subname": "Dup"}, {"subname": "Dup"}],
            }
            await self.db.addobject("parent", data)

            rows = await self._fetch_all_rows("parent_items")
            # Both entries share identical data → only 1 row should survive.
            self.assertEqual(
                len(rows), 1,
                "Duplicate list entries with identical data should be deduplicated",
            )

        self.loop.run_until_complete(run())

    def test_list_field_does_not_create_column_in_parent_table(self):
        """The list key must NOT appear as a column in the parent table —
        it is stored in a child table instead."""
        async def run():
            data = {
                "title": "Parent",
                "items": [{"subname": "A"}],
            }
            await self.db.addobject("parent", data)
            cols = await self._column_names("parent")
            self.assertNotIn(
                "items", cols,
                "List field 'items' must not become a column in the parent table",
            )

        self.loop.run_until_complete(run())

    def test_addobject_with_mock_tracks_calls_per_list_item(self):
        """Classic mock-style check: addobjifnotexist is called once per list
        element, receiving the correct payload each time."""
        async def run():
            called_items = []

            async def mock_addobjifnotexist(name, data, addindex=True, adddate=True):
                called_items.append((name, dict(data)))
                return 100  # fake child ID

            self.db.addobjifnotexist = mock_addobjifnotexist

            data = {
                "title": "Parent Object",
                "items": [{"subname": "A"}, {"subname": "B"}],
            }
            await self.db.addobject("parent", data)

            # Two list items → two mock calls
            self.assertEqual(len(called_items), 2)

            child_table_names = {name for name, _ in called_items}
            self.assertEqual(child_table_names, {"parent_items"})

            subnames = {d["subname"] for _, d in called_items}
            self.assertEqual(subnames, {"A", "B"})

            # Every call must have received the parent's row-id as foreign key
            for _, d in called_items:
                self.assertIn("parent_id", d)

        self.loop.run_until_complete(run())

if __name__ == "__main__":
    unittest.main(verbosity=2)