"""
This module is to upload csv file to DataBase
"""
import logging
import pandas as pd
from mysql.connector import connect, errors


logging.basicConfig(filename="server_info.log", level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

CREATE_TABLE_QUERY = """CREATE TABLE csvfile_data (objectId int(11) NOT NULL,
                        isHighlight tinyint(1) NOT NULL,
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


def csv_to_db_func(file_name):
    """The function reads a file that was uploaded by the user to the server.
        It creates connection to the database and that file was dumped to the database.
        : param file_name :  csv file uploaded by the user."""
    logging.info("Welcome to csv func")
    df_data = pd.read_csv(file_name, index_col=False, delimiter=',')
    replacement = {'height_feet': 0.0, 'height_inches': 0.0,
                   'position': "missing", 'weight_pounds': 0.0}
    df_data.fillna(value=replacement, inplace=True)
    df_data.fillna(0, inplace=True)
    try:
        conn = connect(host='localhost',
                       database="csvfile_upload",
                       user='root',
                       password='yogesh1304')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            logging.info("You're connected to database: %s", record)
            cursor.execute('DROP TABLE IF EXISTS csvfile_data;')
            logging.info("Creating table....")

            cursor.execute(CREATE_TABLE_QUERY)
            logging.info("Table is created....")
            # loop through the data frame
            for i,row in df_data.iterrows():
                print(row)
                cursor.execute("INSERT INTO csvfile_upload.csvfile_data VALUES {}"
                               .format(tuple(row)))
                # the connection is not auto committed by default, so we must commit to
                # save our changes
                conn.commit()
    except errors.DatabaseError as db_e:
        logging.error('%s: %s', db_e.__class__.__name__, db_e)
    except errors.Error as error_e:
        logging.error('%s: %s', error_e.__class__.__name__, error_e)
