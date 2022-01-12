from unittest import TestCase
from unittest.mock import patch
import sys
import os
import mysql.connector
from mysql.connector import errorcode
import crud_operations_db

MYSQL_HOST = "localhost"
MYSQL_DB = "fake_db"
MYSQL_USER = "root"
MYSQL_PASSWORD = "yogesh1304"


class MockDB(TestCase):

    @classmethod
    def setUpClass(cls):
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        )
        cursor = conn.cursor(dictionary=True)

        try:
            cursor.execute(f"DROP DATABASE {MYSQL_DB}")
            cursor.close()
            print("DB dropped")
        except mysql.connector.Error as err:
            print(f"{MYSQL_DB}{err}")

        cursor = conn.cursor()
        try:
            cursor.execute(
                f"CREATE DATABASE {MYSQL_DB}")
        except mysql.connector.Error as err:
            print(f"Failed creating database: {err}")
            sys.exit(1)
        conn.database = MYSQL_DB

        query = """CREATE TABLE test_table (objectId int(11) NOT NULL,isHighlight tinyint(1) NOT NULL,
                        accessionNumber varchar(50) NOT NULL,accessionYear int(11) NOT NULL,
                        isPublicDomain tinyint(1) NOT NULL,primaryImage varchar(50) DEFAULT NULL,
                        primaryImageSmall varchar(50) DEFAULT NULL,additionalImages varchar(50) DEFAULT NULL,
                        department varchar(50) NOT NULL,objectName varchar(50) NOT NULL,
                        title varchar(50) DEFAULT NULL,culture varchar(50) DEFAULT NULL,period varchar(50) DEFAULT NULL,
                        dynasty varchar(50) DEFAULT NULL,reign varchar(50) DEFAULT NULL,portfolio varchar(50) DEFAULT NULL,
                        artistRole varchar(50) DEFAULT NULL,artistPrefix varchar(50) DEFAULT NULL,
                        artistDisplayName varchar(50) DEFAULT NULL,artistDisplayBio varchar(200) DEFAULT NULL,
                        artistSuffix varchar(50) DEFAULT NULL,artistAlphaSort varchar(50) DEFAULT NULL,
                        artistNationality varchar(50) DEFAULT NULL,artistBeginDate varchar(50) DEFAULT NULL,
                        artistEndDate varchar(50) DEFAULT NULL,artistGender varchar(20) NOT NULL,
                        artistWikidata_URL varchar(50) DEFAULT NULL,artistULAN_URL varchar(50) DEFAULT NULL,
                        objectDate varchar(50) DEFAULT NULL,objectBeginDate varchar(50) DEFAULT NULL,
                        objectEndDate varchar(50) DEFAULT NULL,medium varchar(50) DEFAULT NULL,
                        dimensions varchar(50) DEFAULT NULL,measurements varchar(50) DEFAULT NULL,
                        creditLine varchar(50) DEFAULT NULL,geographyType varchar(50) DEFAULT NULL,
                        city varchar(50) DEFAULT NULL,state varchar(50) DEFAULT NULL,
                        county varchar(50) DEFAULT NULL,country varchar(50) DEFAULT NULL,
                        region varchar(50) DEFAULT NULL,subregion varchar(50) DEFAULT NULL,
                        locale varchar(50) DEFAULT NULL,locus varchar(50) DEFAULT NULL,excavation varchar(50) DEFAULT NULL,
                        river varchar(50) DEFAULT NULL,classification varchar(50) DEFAULT NULL,rightsAndReproduction varchar(50) DEFAULT NULL,
                        linkResource varchar(50) DEFAULT NULL,metadataDate varchar(50) DEFAULT NULL,
                        repository varchar(50) DEFAULT NULL,objectURL varchar(50) DEFAULT NULL,
                        tags varchar(50) DEFAULT NULL,objectWikidata_URL varchar(50) DEFAULT NULL,
                        isTimelineWork tinyint(1) NOT NULL,galleryNumber int(11) DEFAULT NULL,constituentID double DEFAULT NULL,
                        role varchar(50) DEFAULT NULL,name varchar(50) DEFAULT NULL,constituentULAN_URL varchar(50) DEFAULT NULL,
                        constituentWikidata_URL varchar(50) DEFAULT NULL,gender varchar(20) NOT NULL,PRIMARY KEY (objectId))
                     """

        try:
            cursor.execute(query)
            conn.commit()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("test_table already exists.")
            else:
                print(err.msg)
        else:
            print("OK")
        cursor.close()
        conn.close()

        testconfig = {
            'host': MYSQL_HOST,
            'user': MYSQL_USER,
            'password': MYSQL_PASSWORD,
            'database': MYSQL_DB
        }
        cls.mock_db_config = patch.dict(CRUD_operations_db.config, testconfig)

    @classmethod
    def tearDownClass(cls):
        cnx = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        )
        cursor = cnx.cursor()

        # drop test database
        try:
            cursor.execute(f"DROP DATABASE {MYSQL_DB}")
            cnx.commit()
            cursor.close()
        except mysql.connector.Error as err:
            print(f"Database {MYSQL_DB} does not exists. Dropping db failed {err}")
        cnx.close()