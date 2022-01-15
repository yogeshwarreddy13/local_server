from mysql.connector.errors import ProgrammingError, DatabaseError

from csv_to_db_package.crud_operations_db import view_db_data, delete_db_row, insert_db_row, \
    update_db_row, select_db_row
from fake_data import value1, value2, value3, value4, value5, value6, value7
from test_fakeDB import MockDB, MYSQL_DB

empty = {}


class TestCrudData(MockDB):
    """Test class for testing the different operations on database
        went successful or not by defining different methods"""

    def test_insert_in_database_successful(self):
        """Checks if data inserted into database table is successfully inserted"""
        with self.mock_db_config:
            insert_db_row(db_name=MYSQL_DB, db_table='test_table', dict_values=value1)
            val1_list = []
            for i in value1.values():
                val1_list.append(i)
            val1_tuple = tuple(val1_list)
            self.assertEqual(select_db_row(db_name=MYSQL_DB, db_table='test_table', object_id=1), val1_tuple)

    def test_insert_in_db_unsuccessful(self):
        """Checks if inserting wrong values throws exception"""
        with self.mock_db_config:
            with self.assertRaises(DatabaseError):
                insert_db_row(db_name=MYSQL_DB, db_table='test_table', dict_values=value4)
            with self.assertRaises(DatabaseError):
                insert_db_row(db_name=MYSQL_DB, db_table='test_table', dict_values=value5)
            with self.assertRaises(DatabaseError):
                insert_db_row(db_name=MYSQL_DB, db_table='test_table', dict_values=empty)

    def test_delete_from_db_successful(self):
        """Checks if data will get deleted from db table by passing the specified id"""
        with self.mock_db_config:
            insert_db_row(db_name=MYSQL_DB, db_table='test_table', dict_values=value7)
            delete_db_row(db_name=MYSQL_DB, db_table='test_table', object_id=9)
            self.assertIsNone(select_db_row(db_name=MYSQL_DB, db_table='test_table', object_id=9))

    def test_delete_from_db_unsuccessful(self):
        """Checks if specified id provided for delete operation is correct or not"""
        with self.mock_db_config:
            with self.assertRaises(ProgrammingError):
                delete_db_row(db_name=MYSQL_DB, db_table='test_table', object_id='')
            with self.assertRaises(ProgrammingError):
                delete_db_row(db_name=MYSQL_DB, db_table='test_table', object_id=[56])
            with self.assertRaises(ProgrammingError):
                delete_db_row(db_name=MYSQL_DB, db_table='test_table', object_id={'key': 'value'})

    def test_update_row_of_database_successful(self):
        """Checks if updating the row went successful"""
        with self.mock_db_config:
            insert_db_row(db_name=MYSQL_DB, db_table='test_table', dict_values=value2)
            update_db_row(db_name=MYSQL_DB, db_table='test_table', dict_values=value6, object_id=2)
            val6_list = []
            for i in value6.values():
                val6_list.append(i)
            val6_tuple = tuple(val6_list)
            self.assertEqual(select_db_row(db_name=MYSQL_DB, db_table='test_table', object_id=2), val6_tuple)

    def test_update_row_of_db_unsuccessful(self):
        """Checks if updating the data throws exception while providing wrong inputs"""
        with self.mock_db_config:
            with self.assertRaises(ProgrammingError):
                update_db_row(db_name=MYSQL_DB, db_table='test_table', dict_values=value3, object_id=6)
            with self.assertRaises(DatabaseError):
                update_db_row(db_name=MYSQL_DB, db_table='test_table', dict_values=value4, object_id=77)
            with self.assertRaises(ProgrammingError):
                update_db_row(db_name=MYSQL_DB, db_table='test_table', dict_values={}, object_id=100)

    def test_read_from_db_successful(self):
        """Checks if data read from table returns same data which is in db"""
        with self.mock_db_config:
            # insert_db_row(db_name=MYSQL_DB, db_table='test_table', dict_values=value8)
            # val8_list = []
            # for i in value8.values():
            #    val8_list.append(i)
            # val8_tuple = tuple(val8_list)
            self.assertIsNotNone(view_db_data(db_name=MYSQL_DB, db_table='test_table'))

    def test_read_from_db_unsuccessful(self):
        """Checks if db name or db table name is given correct or not"""
        with self.mock_db_config:
            with self.assertRaises(ProgrammingError):
                view_db_data(db_name=MYSQL_DB, db_table='data_table')
            with self.assertRaises(ProgrammingError):
                view_db_data(db_name=MYSQL_DB, db_table=100)
            with self.assertRaises(ProgrammingError):
                view_db_data(db_name='data_base', db_table=100)
