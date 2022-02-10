"""
This module contains useful CRUD operation functions that
can be used for database table.
"""
import logging
from mysql.connector import connect, errors
import pymysql
import pandas
import boto3
import os
import csv
import requests
import json

logging.basicConfig(filename='server_info.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

config = {"host": "localhost",
          "user": "root",
          "database": "csvfile_upload",
          "password": "yogesh1304"}

"""The script contains different functions for CRUD operations in database."""


def view_db_data(url):
    """The function creates the data table when the file was uploaded on the browser.
        :param db_name: database name in string format
        :param db_table: database table name in string format
        :return : a table format of file uploaded on browser itself."""
    try:
        # if db_name is not None and db_table is not None:
        #     conn = connect(host='localhost',
        #                    # database="csvfile_upload",
        #                    user='root',
        #                    password='yogesh1304')
        #
        #     if conn.is_connected():
        #         cursor = conn.cursor()
        #
        #         cursor.execute("SELECT * FROM {}.{}".format(db_name, db_table))
        #         columns = cursor.column_names
        #         data = cursor.fetchall()
        #         conn.close()
                get_from_url = requests.get(url)
                get_from_url = json.loads(get_from_url.text)
                columns = tuple(get_from_url["body"][0].keys())
                data = []
                for i in get_from_url["body"]:
                    data.append(tuple(i.values()))
                html_table = ''
                html_table += '<table><tr>'
                for i, _ in enumerate(columns):
                    html_table += '<th id="thead_{}">{}</th>'.format(i, columns[i])
                html_table += '<td><button type="submit" onclick="redirect_to_create()">'\
                              'Create</button></td>'
                html_table += '<td><button type="submit" onclick="redirect_to_S3()">' \
                              'store data to S3</button></td>'
                html_table += '</tr>'
                for rows, _ in enumerate(data):
                    html_table += '<tr id={}>'.format(rows)
                    for item in range(len(data[rows])):
                        html_table += '<td class="row-data"> {}'\
                                      '</td>'.format(data[rows][item])
                    html_table += '<td><button type="button" onclick="my_button_click_handler()">'\
                                  'Delete</button>'
                    html_table += '</td>'
                    html_table += '<td><button type="submit" onclick="my_update_data()">'\
                                  'Update</button></td>'
                    html_table += '</tr>'
                html_table += '</table>'

                return html_table
        # return "Db {} or Table {} doesn't exist".format(db_name, db_table)

    except errors.ProgrammingError as db_e:
        logging.error('%s: %s', db_e.__class__.__name__, db_e)
        raise
    except errors.Error as err:
        logging.error('%s: %s', err.__class__.__name__, err)
        raise


def delete_db_row(url, object_id):
    """The function deletes the row from the database by pressing delete button from browser.
        :param - database name in string format
        :param - database table name in string format
        :param - objectID which user want to delete
        :return - show the table by removing the particular row"""
    try:
        # if db_name is not None and db_table is not None:
        #     conn = connect(host='localhost',
        #                    # database="csvfile_upload",
        #                    user='root',
        #                    password='yogesh1304')
        #
        #     if conn.is_connected():
        #         cursor = conn.cursor()
        #
        #         cursor.execute("DELETE FROM {}.{} WHERE objectID={}"
        #                        .format(db_name, db_table, object_id))
        #         conn.commit()
        #         conn.close()
        data = object_id
        delete_request = requests.delete(url, data=data)
        return "Deleted Successfully"
        # return None
    except errors.ProgrammingError as db_e:
        logging.error('%s: %s', db_e.__class__.__name__, db_e)
        raise
    except errors.Error as err:
        logging.error('%s: %s', err.__class__.__name__, err)
        raise


def insert_db_row(url, dict_values: dict):
    """The function insert the row in the database.
            :param - database name in string format
            :param - database table name in string format
            :return - show the table by inserting the particular row"""
    try:
        # if db_name is not None and db_table is not None:
        #     conn = connect(host='localhost',
        #                    # database="csvfile_upload",
        #                    user='root',
        #                    password='yogesh1304')
        #
        #     if conn.is_connected():
        #         cursor = conn.cursor(buffered=True)
        #         columns = list(dict_values.keys())
        #         columns = ','.join(columns)
        #         values = [dict_values[col] for col in dict_values]
        #         cursor.execute(f'INSERT INTO {db_name}.{db_table}'
        #                        f'({columns}) VALUES {tuple(values)}')
        #         conn.commit()
        get_from_url = requests.get(url)
        get_from_url = json.loads(get_from_url.text)
        columns = tuple(get_from_url["body"][0].keys())
        data = {}
        for i in columns:
            data[i] = dict_values[i]

        post_request = requests.post(url, json=data)
        return "Successfully Inserted"
        # return None
    except errors.IntegrityError as in_err:
        logging.error('%s: %s', in_err.__class__.__name__, in_err)
        raise
    except errors.DatabaseError as db_err:
        logging.error('%s: %s', db_err.__class__.__name__, db_err)
        raise
    except errors.Error as err:
        logging.error('%s: %s', err.__class__.__name__, err)
        raise


def update_db_row(url, dict_values: dict, object_id):
    """The function updates the row from the database by pressing update button from browser.
            :param - database name in string format
            :param - database table name in string format
            :param - dictionary of all values of a particular row selected by the user
            :param - objectID which user want to delete
            :return - show the table by updating the particular row"""
    try:
        # if db_name is not None and db_table is not None:
        #     conn = connect(host='localhost',
        #                    # database="csvfile_upload",
        #                    user='root',
        #                    password='yogesh1304')
        #
        #     if conn.is_connected():
        #         cursor = conn.cursor()
        #
        #         d_values = [f"{key}" + "=" + f"'{dict_values[key]}'" for key in dict_values]
        #         join_values = ','.join(d_values)
        #         cursor.execute(f"UPDATE {db_name}.{db_table} SET {join_values} "
        #                        f"WHERE objectID={object_id}")
        #         conn.commit()
        #         conn.close()
        data = {"target": {"objectID": f'{object_id}'}, "set": dict_values}
        put_request = requests.put(url, json=data)
        return "Data updated successfully"
        # return None
    except errors.IntegrityError as in_err:
        logging.error('%s: %s', in_err.__class__.__name__, in_err)
        raise
    except errors.ProgrammingError as db_e:
        logging.error('%s: %s', db_e.__class__.__name__, db_e)
        raise
    except errors.Error as err:
        logging.error('%s: %s', err.__class__.__name__, err)
        raise


def select_db_row(url, object_id):
    """The function is used to get the row from the database by pressing update
        for the prefilling of the data to the new page to update it.
            :param - database name in string format
            :param - database table name in string format
            :param - objectID which user want to update
            :return - show the prefilled form on the new page to update the data"""
    try:
        # if db_name is not None and db_table is not None:
        #     conn = connect(host='localhost',
        #                    database="csvfile_upload",
        #                    user='root',
        #                    password='yogesh1304')
        #
        #     output = ''
        #     if conn.is_connected():
        #         cursor = conn.cursor()
        #
        #         cursor.execute("SELECT * FROM {}.{} WHERE objectID={}"
        #                        .format(db_name, db_table, object_id))
        #         columns = cursor.column_names
        #         value = cursor.fetchone()
        output = ''
        get_from_url = requests.get(url)
        get_from_url = json.loads(get_from_url.text)
        columns = tuple(get_from_url["body"][0].keys())
        for i, _ in enumerate(get_from_url["body"]):
            if int(get_from_url["body"][i]["objectID"]) == int(object_id):
                value = [value for value in get_from_url["body"][i].values()]
        for i, _ in enumerate(columns):
            output += f'{columns[i]} <input name="{columns[i]}" type="text" ' \
                      f'value="{value[i]}"><br>'
        output += '<button type="submit" onclick="redirect_to_viewtable()">Update</button>'
        # conn.close()
        return output
        # return None
    except errors.DatabaseError as db_e:
        logging.error('%s: %s', db_e.__class__.__name__, db_e)
        raise
    except errors.Error as err:
        logging.error('%s: %s', err.__class__.__name__, err)
        raise


def upload_to_s3(csv_file):

    # conn = pymysql.connect(host='localhost', user='root', password = 'yogesh1304', db='csvfile_upload')
    # cursor = conn.cursor()
    # query = 'select * from csvfile_data'
    # cursor.execute(query)
    # with open("output.csv", "w") as outfile:
    #     writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
    #     writer.writerow(col[0] for col in cursor.description)
    #     for row in cursor:
    #         writer.writerow(row)
    s3_client = boto3.client('s3')

    s3_client.upload_file(csv_file, "yogesh-bucket", "myoutput.csv")

