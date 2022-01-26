#!c:\users\yogeshwar\anaconda3\python.exe
from http.server import HTTPServer, BaseHTTPRequestHandler
import cgi
import logging
from src.csv_to_db_package.csv_to_db import csv_to_db_func
from src.csv_to_db_package.crud_operations_db import view_db_data, delete_db_row, insert_db_row,\
    update_db_row, select_db_row, upload_to_s3
from mysql.connector import connect, errors
# from src.csv_to_db_package.csv_to_db import csv_to_db_func


logging.basicConfig(filename='server_info.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')


class RequestHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        try:
            if self.path.endswith('/uploadCSV'):
                self.send_response(200)
                self.send_header('content-type', 'text/html')
                self.end_headers()

                output = ''
                output += '<html><head><meta charset="utf-8"></head><body>'
                output += '<h1>Welcome to csv upload!!</h1>'
                output += '<h3><a href="/uploadCSV/new">Add new file</a></h3>'
                output += '</body></html>'

                self.wfile.write(output.encode())

            if self.path.endswith('/new'):
                self.send_response(200)
                self.send_header('content-type', 'text/html')
                self.end_headers()

                output = ''
                output += '<html><head><meta charset="utf-8"></head><body>'
                output += '<h2>Add new file</h2>'
                output += '<form method="POST" enctype="multipart/form-data" action="/uploadCSV/new">'
                output += '<input name="task" type="file" placeholder="Add new file">'
                output += '<input type="submit" value="Upload">'
                output += '</form>'
                output += '</body></html>'

                self.wfile.write(output.encode())

            if self.path.endswith('/viewtable'):
                self.send_response(200)
                self.send_header('content-type', 'text/html')
                self.end_headers()

                output = ''
                output += '<html><head><meta charset="utf-8"></head><body>'
                output += '<h2>File uploaded Successfully</h2>'
                output += '<script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script>'
                output += """<script>
                        function my_button_click_handler()
                        {

                            var rowId = event.target.parentNode.parentNode.id;
                            var data = document.getElementById(rowId).querySelectorAll(".row-data");
                            var objectID = data[0].innerHTML;
                            alert('Button Clicked with row_id =' + rowId + ' objectID =as' + objectID);
                            $.ajax(
                                {
                                    type:'POST',
                                    contentType:'application/json;charset-utf-08',
                                    dataType:'json',
                                    url:'http://localhost:8000/delete_data?value='+objectID,

                                }
                            );

                        }
                        function my_update_data()
                        {

                            var rowId = event.target.parentNode.parentNode.id;
                            var data = document.getElementById(rowId).querySelectorAll(".row-data");
                            var objectID = data[0].innerHTML;

                            alert('Button Clicked with row_id =' + rowId + ' objectID =as' + objectID);
                            window.location.href="/update_data/objectID="+objectID

                        }

                        </script>"""
                output += """<script>
                        function redirect_to_create()
                        {
                            window.location.href="/add"
                        }
                        </script>"""
                output += """<script>
                        function redirect_to_S3()
                        {
                            window.location.href="/to_S3"
                        }
                        </script>"""
                output += view_db_data('csvfile_upload', 'csvfile_data')
                output += '</body></html>'

                self.wfile.write(output.encode())

            if self.path.endswith('/to_S3'):
                self.send_response(200)
                self.send_header('content-type', 'text/html')
                self.end_headers()

                output = ''
                output += '<html><head><meta charset="utf-8"></head><body>'
                output += '<form method="POST" enctype="multipart/form-data" action="/to_S3">'
                output += '<h2>Click to store data in S3</h2>'
                output += '<input type="submit" value="Upload to S3">'
                output += '</form>'
                output += '</body></html>'

                self.wfile.write(output.encode())

            if self.path.endswith('/add'):
                self.send_response(200)
                self.send_header('content-type', 'text/html')
                self.end_headers()

                output = ''
                output += '<html><head><meta charset="utf-8"></head><body>'
                output += '<form method="POST" enctype="multipart/form-data" action="/add">'
                output += 'objectID: <input name="objectID" type="text"><br><br>'
                output += 'isHighlight: <input name="isHighlight" type="text"><br><br>'
                output += 'accessionNumber: <input name="accessionNumber" type="text"><br><br>'
                output += 'accessionYear: <input name="accessionYear" type="text"><br><br>'
                output += 'isPublicDomain: <input name="isPublicDomain" type="text"><br><br>'
                output += 'primaryImage: <input name="primaryImage" type="text"><br><br>'
                output += 'primaryImageSmall: <input name="primaryImageSmall" type="text"><br><br>'
                output += 'additionalImages: <input name="additionalImages" type="text"><br><br>'
                output += 'department: <input name="department" type="text"><br><br>'
                output += 'objectName: <input name="objectName" type="text"><br><br>'
                output += 'title: <input name="title" type="text"><br><br>'
                output += 'culture: <input name="culture" type="text"><br><br>'
                output += 'period: <input name="period" type="text"><br><br>'
                output += 'dynasty: <input name="dynasty" type="text"><br><br>'
                output += 'reign: <input name="reign" type="text"><br><br>'
                output += 'portfolio: <input name="portfolio" type="text"><br><br>'
                output += 'artistRole: <input name="artistRole" type="text"><br><br>'
                output += 'artistPrefix: <input name="artistPrefix" type="text"><br><br>'
                output += 'artistDisplayName: <input name="artistDisplayName" type="text"><br><br>'
                output += 'artistDisplayBio: <input name="artistDisplayBio" type="text"><br><br>'
                output += 'artistSuffix: <input name="artistSuffix" type="text"><br><br>'
                output += 'artistAlphaSort: <input name="artistAlphaSort" type="text"><br><br>'
                output += 'artistNationality: <input name="artistNationality" type="text"><br><br>'
                output += 'artistBeginDate: <input name="artistBeginDate" type="text"><br><br>'
                output += 'artistEndDate: <input name="artistEndDate" type="text"><br><br>'
                output += 'artistGender: <input name="artistGender" type="text"><br><br>'
                output += 'artistWikidata_URL: <input name="artistWikidata_URL" type="text"><br><br>'
                output += 'artistULAN_URL: <input name="artistULAN_URL" type="text"><br><br>'
                output += 'objectDate: <input name="objectDate" type="text"><br><br>'
                output += 'objectBeginDate: <input name="objectBeginDate" type="text"><br><br>'
                output += 'objectEndDate: <input name="objectEndDate" type="text"><br><br>'
                output += 'medium: <input name="medium" type="text"><br><br>'
                output += 'dimensions: <input name="dimensions" type="text"><br><br>'
                output += 'measurements: <input name="measurements" type="text"><br><br>'
                output += 'creditLine: <input name="creditLine" type="text"><br><br>'
                output += 'geographyType: <input name="geographyType" type="text"><br><br>'
                output += 'city: <input name="city" type="text"><br><br>'
                output += 'state: <input name="state" type="text"><br><br>'
                output += 'county: <input name="county" type="text"><br><br>'
                output += 'country: <input name="country" type="text"><br><br>'
                output += 'region: <input name="region" type="text"><br><br>'
                output += 'subregion: <input name="subregion" type="text"><br><br>'
                output += 'locale: <input name="locale" type="text"><br><br>'
                output += 'locus: <input name="locus" type="text"><br><br>'
                output += 'excavation: <input name="excavation" type="text"><br><br>'
                output += 'river: <input name="river" type="text"><br><br>'
                output += 'classification: <input name="classification" type="text"><br><br>'
                output += 'rightsAndReproduction: <input name="rightsAndReproduction" type="text"><br><br>'
                output += 'linkResource: <input name="linkResource" type="text"><br><br>'
                output += 'metadataDate: <input name="metadataDate" type="text"><br><br>'
                output += 'repository: <input name="repository" type="text"><br><br>'
                output += 'objectURL: <input name="objectURL" type="text"><br><br>'
                output += 'tags: <input name="tags" type="text"><br><br>'
                output += 'objectWikidata_URL: <input name="objectWikidata_URL" type="text"><br><br>'
                output += 'isTimelineWork: <input name="isTimelineWork" type="text"><br><br>'
                output += 'galleryNumber: <input name="galleryNumber" type="text"><br><br>'
                output += 'constituentID: <input name="constituentID" type="text"><br><br>'
                output += 'role: <input name="role" type="text"><br><br>'
                output += 'name: <input name="name" type="text"><br><br>'
                output += 'constituentULAN_URL: <input name="constituentULAN_URL" type="text"><br><br>'
                output += 'constituentWikidata_URL: <input name="constituentWikidata_URL" type="text"><br><br>'
                output += 'gender: <input name="gender" type="text"><br><br>'
                output += '<input type="submit" value="Add">'
                output += '</form>'
                output += '</body></html>'

                self.wfile.write(output.encode())

            if self.path.startswith('/update_data'):
                value = str(self.path[22:])[3:]

                self.send_response(200)
                self.send_header('content-type', 'text/html')
                self.end_headers()

                output = ''
                output += '<html><body>'
                output += '<form method="POST" enctype="multipart/form-data" action="/update_data">'
                output += select_db_row('csvfile_upload', 'csvfile_data', int(value))
                output += """<script>
                                    function redirect_to_viewtable()
                                    {
                                        window.location.href="/viewtable"
                                    }
                                    </script>"""
                output += '</form>'
                output += '</body></html>'

                self.wfile.write(output.encode())
        except PermissionError as per_err:
            logging.error('%s: %s', per_err.__class__.__name__, per_err)
        except TypeError as type_err:
            logging.error('%s: %s', type_err.__class__.__name__, type_err)
        except Exception as err:
            logging.error('%s: %s', err.__class__.__name__, err)

    def do_POST(self):
        try:
            if self.path.endswith('/new'):
                ctype, pdict = cgi.parse_header(self.headers.get('content-type'))
                pdict['boundary'] = bytes(pdict['boundary'], "utf-8")
                content_len = int(self.headers.get('Content-length'))
                pdict['CONTENT-LENGTH'] = content_len
                if ctype == 'multipart/form-data':
                    fields = cgi.parse_multipart(self.rfile, pdict)
                    file = fields.get('task')[0]
                    file = file.decode("cp1252")

                    with open('file.csv', mode='w', encoding='utf-8') as f:
                        for data in file.split('\r\r'):
                            f.write(data)

                    csv_to_db_func('file.csv')
                self.send_response(301)
                self.send_header('content-type', 'text/html')
                self.send_header('Location', '/viewtable')
                self.end_headers()

                self.wfile.write(file.encode())

            if self.path.startswith('/delete_data'):
                value_id = self.path[22:]

                delete_db_row('csvfile_upload', 'csvfile_data', value_id)
                self.send_response(301)
                self.send_header('content-type', 'text/html')
                self.send_header('Location', '/viewtable')
                self.end_headers()

            if self.path.startswith('/to_S3'):

                upload_to_s3()
                self.send_response(301)
                self.send_header('content-type', 'text/html')
                self.send_header('Location', '/viewtable')
                self.end_headers()

            if self.path.endswith('/add'):
                ctype, pdict = cgi.parse_header(self.headers.get('content-type'))
                pdict['boundary'] = bytes(pdict['boundary'], "utf-8")
                content_len = int(self.headers.get('Content-length'))
                pdict['CONTENT-LENGTH'] = content_len
                if ctype == 'multipart/form-data':
                    fields = cgi.parse_multipart(self.rfile, pdict)
                    i=0
                    for key in fields:
                        if fields[key][0] == '':
                            i = i+1

                    if i != 0:

                        self.send_response(200)
                        self.send_header('content-type', 'text/html')
                        self.end_headers()

                        output = ''
                        output += '<html><head><meta charset="utf-8"></head><body>'
                        output += '<h2>All fields must be filled</h2>'
                        output += '<h3><a href="/viewtable">Back to viewtable</a></h3>'
                        output += '</body></html>'

                        self.wfile.write(output.encode())
                    else:
                        for key in fields:
                            fields[key] = fields[key][0]

                        print(fields)
                        insert_db_row('csvfile_upload', 'csvfile_data', fields)

                self.send_response(301)
                self.send_header('content-type', 'text/html')
                self.send_header('Location', '/viewtable')
                self.end_headers()

            if self.path.startswith('/update_data'):
                ctype, pdict = cgi.parse_header(self.headers.get('content-type'))
                pdict['boundary'] = bytes(pdict['boundary'], "utf-8")
                content_len = int(self.headers.get('Content-length'))
                pdict['CONTENT-LENGTH'] = content_len
                if ctype == 'multipart/form-data':
                    fields = cgi.parse_multipart(self.rfile, pdict)
                    conn = connect(host='localhost',
                                   database="csvfile_upload",
                                   user='root',
                                   password='yogesh1304')

                    if conn.is_connected():
                        cursor = conn.cursor()
                        query = "SELECT objectId From csvfile_upload.csvfile_data"
                        cursor.execute(query)
                        primary_keys = cursor.fetchall()
                        primary_key = fields['objectId']
                        list1 = []
                        for i in range(len(primary_keys)):
                            list1.append(i)
                        list2 = []
                        for i in range(len(list1)):
                            list2.append(str(primary_keys[i][0]))
                        if primary_key[0] in list2:

                            for key in fields:
                                fields[key] = fields[key][0]
                            update_db_row('csvfile_upload', 'csvfile_data', fields, int(fields['objectId']))
                        else:
                            self.send_response(200)
                            self.send_header('content-type', 'text/html')
                            self.end_headers()

                            output = ''
                            output += '<html><head><meta charset="utf-8"></head><body>'
                            output += '<h2>You cannot update primary key</h2>'
                            output += '<h3><a href="/viewtable">Back to viewtable</a></h3>'
                            output += '</body></html>'

                            self.wfile.write(output.encode())

                self.send_response(301)
                self.send_header('content-type', 'text/html')
                self.send_header('Location', '/viewtable')
                self.end_headers()
        except PermissionError as per_err:
            logging.error('%s: %s', per_err.__class__.__name__, per_err)
        except TypeError as type_err:
            logging.error('%s: %s', type_err.__class__.__name__, type_err)
        except Exception as err:
            logging.error('%s: %s', err.__class__.__name__, err)


def main():
    port = 8000
    server = HTTPServer(('', port), RequestHandler)
    print("Server started on localhost: ", port)
    server.serve_forever()


if __name__ == "__main__":
    main()
