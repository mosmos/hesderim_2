import logging
import pyodbc
import os
import subprocess
import json
from requests import request,auth
import sys
import argparse
import secrets
import base64
from datetime import datetime as dt, timedelta
from osgeo import gdal
import glob

#gdal.SetCacheMax(512) # TODO try bigger RAM 
logging.basicConfig(filename='my_app.log', level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
                    
class ResponseMessage:

    def __init__(self, message="", response_code=200):
        self.message = message
        self.response_code = response_code

    def update_message(self, new_message, new_response_code=200):
        self.message = new_message
        self.response_code = new_response_code

    def send_response_as_dict(self):
        return {"Message": self.message, "Response Code": self.response_code}


class Publish_Hsederim(object):

    responseMessage = ResponseMessage()
    dirname = os.path.dirname(__file__)
    
    
    def getArguments(self):
        """
        Get command line arguments to set relevant required for actions to run
        Takes all arguments when the script is run through the CLI.
        In gisprod01 is run like the following example:

        python D:\raster_publisher_api\publish_hesderim_api.py -W http://10.1.70.224:8080/geoserver/rest -U admin -P telaviv -o \\gisprod01\d$\GeoServer\data_dir\coverages\hesderim
        """
        try:
            parser = argparse.ArgumentParser()
            parser.add_argument("-V", "--version",
                                help="show program version", action="store_true")
            parser.add_argument("--url", "-W", 
                                help="set GeoServer REST base URL, requires -user and -password")
            parser.add_argument("--user", "-U", 
                                help="set GeoServer User")
            parser.add_argument("--password", "-P", 
                                help="set GeoServer Password")
            parser.add_argument("--output", "-o", 
                                help="Set output directory for generated rasters")

            self.gs_base_url = "http://10.6.70.120:8080/geoserver/rest"   
            self.geoserver_user = "admin"   
            self.geoserver_pass = "telaviv"   
            self.output_dir = r"\\gisppr01\d$\GeoServer\data_dir\coverages\hesderim"  

            # # Read arguments from the command line
            # args = parser.parse_args()
            #
            #
            # # Check for --version or -V
            # if args.version:
            #     print("{}".format(version))
            #     sys.exit()
            #
            # if args.url:
            #     self.gs_base_url = args.url
            # else:
            #     self.gs_base_url = "http://10.1.70.224:8080/geoserver/rest"  # gisprod01
            #
            # if args.user:
            #     self.geoserver_user = args.user
            # else:
            #     self.geoserver_user = "admin"  # gisprod01
            #
            # if args.password:
            #     self.geoserver_pass = args.password
            # else:
            #     self.geoserver_pass = "telaviv"  # gisprod01
            #
            # if args.output:
            #     self.output_dir = r"{}".format(args.output)
            # else:
            #     # for gisdev01 r"\\gisdev01\d$\GeoServer\data_dir\coverages\hesderim"
            #     # output_dir = r"\\gisppr01\d$\GeoServer\data_dir\coverages\hesderim"
            #     self.output_dir= r"\\gisdev01\d$\GeoServer\data_dir\coverages\hesderim"
            # # return gs_base_url, geoserver_user, geoserver_pass, output_dir

        except Exception as e:
            logging.error(e, exc_info=True)


    # gs_base_url, geoserver_user, geoserver_pass, output_dir = getArguments()
    def check_workspace_exists(self,ws_name):
        """
        Check if the geoserver workspace exists
        If workspace does not exist, runs `create_workspace`

        See:
            https://docs.geoserver.org/latest/en/api/#1.0.0/workspaces.yaml

        Args:
            ws_name (str): workspace name

        Returns:
            (tuple): tuple containing:
                status_code (int): Status code
                msg (str): Status message
        """
        url = self.gs_base_url + "/workspaces/{}.json".format(ws_name)
        response = request("GET", url, auth=auth.HTTPBasicAuth(
            self.geoserver_user, self.geoserver_pass))
        if response.status_code == 200:
            msg = "ws exists"
            logging.info(msg)
            return 200, msg
        else:
            return self.create_workspace(ws_name)


    def create_workspace(self,ws_name):
        """
        See:
            https://docs.geoserver.org/latest/en/api/#1.0.0/workspaces.yaml
        Args:
            ws_name (str): workspace name
        Returns:
            (tuple): tuple containing:
                status_code (int): Status code returned from GeoServer REST API
                msg (str): Status message
        """
        url = self.gs_base_url + "/workspaces"
        payload = {
            "workspace": {
                "name": "{}".format(ws_name)
            }
        }
        headers = {
            "Content-Type": "application/json"
        }
        response = request("POST", url, auth=auth.HTTPBasicAuth(
            self.geoserver_user, self.geoserver_pass), headers=headers, data=json.dumps(payload))
        if response.status_code == 201:
            msg = "Created workspace: '{}'".format(ws_name)
            logging.info(msg)
            response_code = 201
        else:
            response_code = 500
            msg = f"Error creating workspace: GeoServer returned status code: {response.status_code}"
            self.responseMessage.update_message(
                new_message=msg, new_response_code=500)

            logging.error(msg)
        return response_code, msg


    def check_coveragestore_exists(self,ws_name, coveragename, path):
        """
        Check if a GeoServer coveragestore exists using the REST API.
        If workspace does not exist, runs `create_coveragestore` with the same parameters.
        CoverageStores are GeoServer raster data soruces, multiple layers can be created from publishing a single data source multiple times.

        See:
            https://docs.geoserver.org/latest/en/api/#1.0.0/coveragestores.yaml


        Args:
            ws_name (str): workspace name
            coveragename (str): name of the coveragestore
            path (str): path to the GeoTiff file

        Returns:
            (tuple): tuple containing:
                status_code (int): Status code returned from GeoServer REST API
                msg (str): Status message
        """
        url = self.gs_base_url + \
            "/workspaces/{}/coveragestores/{}.json".format(ws_name, coveragename)
        response = request("GET", url, auth=auth.HTTPBasicAuth(
            self.geoserver_user, self.geoserver_pass))
        if response.status_code == 200:
            msg = "coveragestore exists"
            logging.info(msg)
            return 200, msg
        else:
            return self.create_coveragestore(ws_name, coveragename, path)


    def create_coveragestore(self,ws_name, coveragename, path):
        """
        Create a GeoServer coveragestore using the REST API from a GeoTiff.
        CoverageStores are GeoServer raster data soruces, multiple layers can be created from publishing a single data source multiple times.
        File needs to be accessible to GeoServer.

        See:
            https://docs.geoserver.org/latest/en/api/#1.0.0/coveragestores.yaml


        Args:
            ws_name (str): workspace name
            coveragename (str): name of the coveragestore
            path (str): path to the GeoTiff file

        Returns:
            (tuple): tuple containing:
                status_code (int): Status code returned from GeoServer REST API
                msg (str): Status message
        """
        url = self.gs_base_url + "/workspaces/{}/coveragestores".format(ws_name)
        payload = {
            "coverageStore": {
                "workspace": ws_name,
                "type": "GeoTIFF",
                "enabled": "true",
                "name": coveragename,
                "url": "file:{}".format(path)
            }
        }
        headers = {
            "Content-Type": "application/json"
        }
        response = request("POST", url, auth=auth.HTTPBasicAuth(
            self.geoserver_user, self.geoserver_pass), headers=headers, data=json.dumps(payload))
        if response.status_code == 201:
            msg = "Created coverageStore: '{}'".format(coveragename)
            logging.info(msg)
            response_code = 201
        else:
            response_code = 500
            msg = f"Could not create coveragestore. GeoServer returned status code: {response.status_code} : {response}"
            self.responseMessage.update_message(
                new_message=msg, new_response_code=500)
            logging.info(msg)

        return response_code, msg


    def check_coveragelayer_exists(self,ws_name, coveragename):
        """
        Check if a GeoServer coverage layer exists using the REST API.
        Coverages are GeoServer raster layers.
        A coveragestore needs to be created before a coverage layer is created.

        See:
            https://docs.geoserver.org/latest/en/api/#1.0.0/coverages.yaml


        Args:
            ws_name (str): workspace name
            coveragename (str): name of the coveragestore

        Returns:
            (tuple): tuple containing:
                status_code (int): Status code returned from GeoServer REST API
                msg (str): Status message
        """
        url = self.gs_base_url + \
            "/workspaces/{}/coveragestores/{}/coverages.json".format(
                ws_name, coveragename)

        response = request("GET", url, auth=auth.HTTPBasicAuth(
            self.geoserver_user, self.geoserver_pass))
        if response.status_code == 200 and len(response.json()['coverages']) > 0:
            msg = "coverage layer exists"
            logging.info(msg)
            return 200, msg
        else:
            return self.create_coveragelayer(ws_name, coveragename)


    def create_coveragelayer(self,ws_name, coveragename):
        """
        Check if a GeoServer coverage layer exists using the REST API.
        Coverages are GeoServer raster layers, the difference from vector layers being the specification of the JSON payload.
        A coveragestore needs to be created before a coverage layer is created.

        See:
            https://docs.geoserver.org/latest/en/api/#1.0.0/coverages.yaml


        Args:
            ws_name (str): workspace name
            coveragename (str): name of the coveragestore

        Returns:
            (tuple): tuple containing:
                status_code (int): Status code returned from GeoServer REST API
                msg (str): Status message
        """
        url = self.gs_base_url + \
            "/workspaces/{}/coveragestores/{}/coverages".format(
                ws_name, coveragename)
        payload = {
            "coverage": {
                "name": coveragename,
                "nativeName": coveragename,
                "namespace": {
                    "name": ws_name,
                    "href": "{}/namespaces/{}.json".format(self.gs_base_url, ws_name)
                },
                "title": coveragename,
                "description": "Generated from GeoTIFF",
                "nativeCRS": {
                    "@class": "projected",
                    "$": "PROJCS[\"Israeli Grid 05/12\", \r\n  GEOGCS[\"IG05/12 Intermediate CRS\", \r\n    DATUM[\"IG05/12 Intermediate Datum\", \r\n      SPHEROID[\"GRS 1980\", 6378137.0, 298.257222101, AUTHORITY[\"EPSG\",\"7019\"]], \r\n      AUTHORITY[\"EPSG\",\"1144\"]], \r\n    PRIMEM[\"Greenwich\", 0.0, AUTHORITY[\"EPSG\",\"8901\"]], \r\n    UNIT[\"degree\", 0.017453292519943295], \r\n    AXIS[\"Geodetic longitude\", EAST], \r\n    AXIS[\"Geodetic latitude\", NORTH], \r\n    AUTHORITY[\"EPSG\",\"6990\"]], \r\n  PROJECTION[\"Transverse_Mercator\", AUTHORITY[\"EPSG\",\"9807\"]], \r\n  PARAMETER[\"central_meridian\", 35.20451694444444], \r\n  PARAMETER[\"latitude_of_origin\", 31.734393611111106], \r\n  PARAMETER[\"scale_factor\", 1.0000067], \r\n  PARAMETER[\"false_easting\", 219529.584], \r\n  PARAMETER[\"false_northing\", 626907.39], \r\n  UNIT[\"m\", 1.0], \r\n  AXIS[\"Easting\", EAST], \r\n  AXIS[\"Northing\", NORTH], \r\n  AUTHORITY[\"EPSG\",\"6991\"]]"
                },
                "srs": "EPSG:6991",
                "enabled": "true",
                "store": {
                    "@class": "coverageStore",
                    "name": "{}:{}".format(ws_name, coveragename),
                    "href": "{}/workspaces/{}/coveragestores/{}.json".format(self.gs_base_url, ws_name, coveragename)
                },
                "supportedFormats": {
                    "string": [
                        "ArcGrid",
                        "GEOTIFF",
                        "S3GeoTiff",
                        "GeoPackage (mosaic)",
                        "ImageMosaic",
                        "GIF",
                        "PNG",
                        "JPEG",
                        "TIFF"
                    ]
                },
                "dimensions": {
                    "coverageDimension": [
                        {
                            "name": "RED_BAND",
                            "description": "GridSampleDimension[-Infinity,Infinity]",
                            "range": {
                                "min": 0,
                                "max": 255
                            },
                            "unit": "W.m-2.Sr-1",
                            "dimensionType": {
                                "name": "UNSIGNED_8BITS"
                            }
                        },
                        {
                            "name": "GREEN_BAND",
                            "description": "GridSampleDimension[-Infinity,Infinity]",
                            "range": {
                                "min": 0,
                                "max": 255
                            },
                            "unit": "W.m-2.Sr-1",
                            "dimensionType": {
                                "name": "UNSIGNED_8BITS"
                            }
                        },
                        {
                            "name": "BLUE_BAND",
                            "description": "GridSampleDimension[-Infinity,Infinity]",
                            "range": {
                                "min": 0,
                                "max": 255
                            },
                            "unit": "W.m-2.Sr-1",
                            "dimensionType": {
                                "name": "UNSIGNED_8BITS"
                            }
                        }
                    ]
                },
                "nativeCoverageName": coveragename
            }
        }
        headers = {
            "Content-Type": "application/json"
        }
        response = request("POST", url, auth=auth.HTTPBasicAuth(
            self.geoserver_user, self.geoserver_pass), headers=headers, data=json.dumps(payload))
        if response.status_code == 201:
            msg = "Created coverage layer: '{}'".format(coveragename)
            logging.info(msg)
            response_code = 201
        else:
            msg = f"Failed to publish layer. GeoServer returned status code: {response.status_code}, {response.text}"
            self.responseMessage.update_message(
                new_message=msg, new_response_code=500)

            logging.error(msg)
        return response_code, msg


    def test_server(self):
        """
        Tests that the `gs_base_url` is set to a valid GeoServer REST URL

        See:
            https://docs.geoserver.org/latest/en/api/#1.0.0/layers.yaml

        Returns:
            (int): Status code of the call to the GeoServer REST URL layers.json, if the server is running should return 200
        """
        url = self.gs_base_url+"/layers.json"
        response = request("GET", url, auth=auth.HTTPBasicAuth(
            self.geoserver_user, self.geoserver_pass))
        return response.status_code
        
        
    def generateRaster(self,input, output):
        
        # copy C:\Program Files\QGIS 3.22.5\apps\Python39\Lib\site-packages\GDAL-3.4.1-py3.9-win-amd64.egg
        # to the local env 
        # copy the osgeo folders out from the egg to the local site-packages
        logging.info(f">>> starting creating raster: {output}")
        logging.info(f">>> from folder: {input}")
        try:
            subprocess.run([
            r"D:\Teum4U\Hesderim_2\env_geoserver\.venv\Scripts\python.exe",
            r"D:\Teum4U\Hesderim_2\generate_raster_gdal.py",
            '-i', input,
            '-o',output],check=True)
            
            logging.info(">>> finished with success")
        
        except subprocess.CalledProcessError as err:
            self.responseMessage.update_message(new_message=f"Failed to create raster", new_response_code=500)
            logging.error(">>> Failed to create raster", exc_info=True)
            logging.error(err, exc_info=True)
            
        except Exception as err:
            self.responseMessage.update_message(new_message=f"general error", new_response_code=500)
            logging.error(">>> general error:", err)
            
            

        
    def getName(self,id, environment):
        """
        For hesderey Tnua'a, using an ID and environment, return the:
        1. name of the service to be used.
        2. path of the input tiles
        3. workspace name to be used in GeoServer

        Args:
            id (int) : id in the relevant database, can change by environment
            environment (str): name of the environment to be used, can be "dev", "test", "ppr" or "prod". "dev" is set by default if none is supplied (before the function is used)

        Returns:
            (tuple): tuple containing:
                hesder_name (str): name of the service to be used.
                input_path (str): path of the input tiles
                ws_name (str): workspace name to be used in GeoServer
        """
        try:
            hesder_name = ''
            input_path = ''
            connString = self.getConnectionString(environment)
            ws_name = self.getWorkSpace(environment)
            conn = pyodbc.connect(connString)
            cursor = conn.cursor()
            cursor.execute(
                "select * from th_t_temp_va_hesder_betochnit where k_temp_va_hesder_betochnit={};".format(id))
            columns = [column[0] for column in cursor.description]
            row = cursor.fetchone()
            if row == None:
                self.responseMessage.update_message(new_message=f"No Hesder found under the ID:{id}",new_response_code=500)
                logging.error(f"No Hesder found under the ID:{id}")
                return None
            row_as_dict = dict(zip(columns, row))
            hesder_name = row_as_dict['t_shem_raster']  # row[-3]
            input_path = row_as_dict['t_path_raster']  # row[-4]
        except pyodbc.DatabaseError as e:
            self.responseMessage.update_message(new_message=f"Database error:{e.args[0]}", new_response_code=500)
            logging.error(e, exc_info=True)
        return hesder_name, input_path, ws_name
        
        
    def getConnectionString(self,environment):
        """
        Get the connection string by the environment name
        Strings need to be recognizable in `pyodbc`, parameters split by `;`

        Args:
            environment (str): nmae of th enviroment to get the connection string for

        Returns:
            (str): A connection string to be used directly in `pyodbc.connect`
        """
        if environment == 'dev':
            connString = 'Driver={SQL Server};'+'Server=sqldev1601;' + \
                'Database=db026_new;'+'UID=db026;'+'PWD=db026;'  # +'Trusted_Connection=yes;'
        elif environment == 'test':
            connString = 'Driver={SQL Server};'+'Server=sqltst1601;'+'Database=db026_new;' + \
                'UID=db026_t;'+'PWD=h#969026;'  # +'Trusted_Connection=yes;'
        elif environment == 'ppr':
            connString = 'Driver={SQL Server};'+'Server=sqlppr1601;'+'Database=db026_new;' + \
                'UID=db026_ppr;'+'PWD=h#511669;'  # +'Trusted_Connection=yes;'
        elif environment == 'prod':
            connString = 'Driver={SQL Server};'+'Server=SQLPRD1601NODE2;' + \
                'Database=db026_new;'+'UID=db026_p;' + \
                'PWD=h#060026;'  # +'Trusted_Connection=yes;'
        else:
            connString = 'Driver={SQL Server};'+'Server=sqldev1601;' + \
                'Database=db026_new;'+'UID=db026;'+'PWD=db026;'  # +'Trusted_Connection=yes;'
        return connString


    def getWorkSpace(self,environment):
        """
        Get the GeoServer workspace name based on the environment name
        Names are agreed upon and used by the Hesderim front end to retreive the service

        Args:
            environment (str): nmae of th enviroment to get the connection string for

        Returns:
            (str): The name of the GeoServer workspace which will contain the service
        """
        if environment == 'dev':
            ws_name = 'hesderim_dev'
        elif environment == 'test':
            ws_name = 'hesderim_test'
        elif environment == 'ppr':
            ws_name = 'hesderim_ppr'
        elif environment == 'prod':
            ws_name = 'hesderim_prod'
        else:
            ws_name = 'hesderim_dev'
        return ws_name


    def publish_hesder(self,id, environment):
        """
        API endpoint meant for creating a single raster file from multiple *.tif tiles created by AutoCAD.\n
        The files is then published as a GeoServer service.
        ---
        Args:
            id (int): id of the service in the Hesderim management tables, used to extract input path and output name
            env (str): working environment, determines which management server and tables are queried and which GeoServer wokspace will be used

        Responses:
            (Object): JSON response and a response code:
            \n\t\t500: Description: Error, description added.
            \n\t\t304: (No Change): Layer exists, recreated data source from files
            \n\t\t201: (Success): Files and service created successfully

            JSON response for errors is always:\n
                {"message": The Error Message}

            JSON response for success is:\n
                {'url': 'http://gisprod01:8080/geoserver/ows',
                        'layerName': '{}:{}'.format(workspace name,layer name),
                        'layerPath': layer path in the output directory,
                        'message': success or no change message,
                        'GeoServer': {
                            'Workspace': {'status': 200/201/500,
                                        'message': "workspace exists"/"Created workspace: '{}'".format(workspace name)/Error Message},
                            'CoverageStore': {'status': 200/201/500,
                                        'message':"coveragestore exists"/"Created coveragestore: '{}'".format(coveragestore name)/Error Message},
                            'CoverageLayer': {'status': 200/201/500,
                                        'message':"coveragelayer exists"/"Created coveragelayer: '{}'".format(layer name)/Error Message}},
                        }}

        """
        self.getArguments()
        response_code = 201
        msg = "Created Succefully"
        try:
            if (id is None):
                self.responseMessage.update_message(
                    new_message=f"No ID provided", new_response_code=400)

                return self.responseMessage.send_response_as_dict()
            logging.info('Hesderim - id: {}, env: {}'.format(id, environment))
            try:
                layerName, tilesPath, ws_name = self.getName(id, environment)
            except:
                return self.responseMessage.send_response_as_dict()
            layerPath = self.output_dir+"/{}.tif".format(layerName)
            output =    self.output_dir+"/{}".format(layerName)
            numTifs = len([name for name in os.listdir(
                tilesPath) if name.endswith('.tif')])
            if (os.path.exists(layerPath)):
                logging.info("{} already exists".format(layerPath))
            else:
                if numTifs > 0:
                    try:
                        logging.info("creating: {} ".format(layerPath))
                        self.generateRaster(tilesPath, output)# output)
                    except:
                        return self.responseMessage.send_response_as_dict()
                else:
                    logging.error(
                        "No tif images in the input folder, '{}' is empty".format(tilesPath))
                    logging.error(
                        "failed to create raster for env: {}, id: {}".format(environment, id))

                    self.responseMessage.update_message(new_message=f"No tif images in the input folder, '{tilesPath}' is empty", new_response_code=500)
                    return self.responseMessage.send_response_as_dict()
            server_response = self.test_server()
            if server_response != 200:

                self.responseMessage.update_message(
                    new_message=f"GeoServer Error, returned status code: {server_response}", new_response_code=500)
                return self.responseMessage.send_response_as_dict()

            response_object = {}
            try:
                ws_status, ws_msg = self.check_workspace_exists(ws_name)
                cs_status, cs_msg = self.check_coveragestore_exists(
                    ws_name, layerName, layerPath)
                cl_status, cl_msg = self.check_coveragelayer_exists(ws_name, layerName)
            except:
                return self.responseMessage.send_response_as_dict()
            if ws_status == 500 or cs_status == 500 or cl_status == 500:
                response_code = 500
            if cl_status == 200:
                response_code = 304
                msg = "Layer exists, raster files recreated"
            if environment == "prod":
                response_url = 'http://gisprod01:8080/geoserver/ows'
            elif environment == "ppr":
                response_url = 'http://gisppr01:8080/geoserver/ows'
            else:
                response_url = 'http://gisdev01:8080/geoserver/ows'
            response_object[ws_name] = {'url': response_url,
                                        'layerName': '{}:{}'.format(ws_name, layerName),
                                        'layerPath': layerPath,
                                        'message': msg,
                                        'GeoServer': {
                                            'Workspace': {'status': ws_status,
                                                          'message': ws_msg},
                                            'CoverageStore': {'status': cs_status,
                                                              'message': cs_msg},
                                            'CoverageLayer': {'status': cl_status,
                                                              'message': cl_msg},
                                        }}
        except Exception as e:
            logging.error(e, exc_info=True)
            response_code = 500
            response_object = {
                'error': str(e)
            }

        return response_object, response_code


    def getTileJsonAddress(self,environment, tilesDir):
        """
        Get the IIS URL for the output raster metadata JSON

        Args:
            environment (str): name of the enviroment to get the server an path
            tilesDir (str): name of the output raster, without extension


        Returns:
            url (str): URL of the output raster's metadata file
        """
        if environment == 'dev':
            url = 'http://gisdevcog01/rasters/dev/{}.json'.format(tilesDir)
        elif environment == 'test':
            url = 'http://gisdevcog01/rasters/tst/{}.json'.format(tilesDir)
        elif environment == 'ppr':
            url = 'http://gisdevcog01/rasters/ppr/{}.json'.format(tilesDir)
        elif environment == 'prd':
            url = 'http://gisprodcog02/rasters/prd/{}.json'.format(tilesDir)
        else:
            url = 'http://gisdevcog01/rasters/dev/{}.json'.format(tilesDir)
        return url
        
        
    def check_publish_hesder(self,id, environment):
        
        self.getArguments()
        response_code = 201
        msg = "finished"
        try:
            if (id is None):
                self.responseMessage.update_message(
                    new_message=f"No ID provided", new_response_code=400)
                return self.responseMessage.send_response_as_dict()
            
            logging.info('Hesderim - id: {}, env: {}'.format(id, environment))

            try:
                layerName, tilesPath, ws_name = self.getName(id, environment)
            except:
                return self.responseMessage.send_response_as_dict()
            
            layerPath = self.output_dir+"/{}.tif".format(layerName)
            #output =    self.output_dir+"/{}".format(layerName)
            #numTifs = len([name for name in os.listdir(
            #    tilesPath) if name.endswith('.tif')])
            if (os.path.exists(layerPath)):
                logging.info("{} already exists".format(layerPath))
            else:
                return self.responseMessage.send_response_as_dict()
                
            server_response = self.test_server()

            if server_response != 200:
                self.responseMessage.update_message(
                    new_message=f"GeoServer Error, returned status code: {server_response}", new_response_code=500)
                return self.responseMessage.send_response_as_dict()

            response_object = {}
            try:
                ws_status, ws_msg = self.check_workspace_exists(ws_name)
                cs_status, cs_msg = self.check_coveragestore_exists(
                    ws_name, layerName, layerPath)
                cl_status, cl_msg = self.check_coveragelayer_exists(ws_name, layerName)
            except:
                return self.responseMessage.send_response_as_dict()
            
            if ws_status == 500 or cs_status == 500 or cl_status == 500:
                response_code = 500
            if cl_status == 200:
                response_code = 304
                msg = "Layer allready exists"
            if environment == "prod":
                response_url = 'http://gisprod01:8080/geoserver/ows'
            elif environment == "ppr":
                response_url = 'http://gisppr01:8080/geoserver/ows'
            else:
                response_url = 'http://gisdev01:8080/geoserver/ows'
            response_object[ws_name] = {'url': response_url,
                                        'layerName': '{}:{}'.format(ws_name, layerName),
                                        'layerPath': layerPath,
                                        'message': msg,
                                        'GeoServer': {
                                            'CoverageLayer': {'status': cl_status,
                                                              'message': cl_msg},
                                        }}
        except Exception as e:
            logging.error(e, exc_info=True)
            response_code = 500
            response_object = {
                'error': str(e)
            }

        return response_object, response_code   


    def create_nativ_json(self,output):
        """
        Create metadata JSON for the generated raster

        Args:
            output (str): path and name of output raster, without extension

        Returns:
            msg (str): result of `create_nativ_json` for success, error message for error
            response_code (int): 201 for success, 400 for rasters with out of globe bounds
        """
        try:
            msg = 'Raster and metdata JSON {} created successfully'.format(output)
            response_code = 201
            cmd5 = subprocess.Popen(
                ["gdalinfo", "-json", "-nofl", "-noct", "{}.tiff".format(output)], stdout=subprocess.PIPE)
            logging.debug(
                "cmd5: 'gdalinfo -json -nofl -noct {}.tiff".format(output))
            jsonS, _ = cmd5.communicate()
            gdal_md = json.loads(jsonS)
            bounds = gdal_md["cornerCoordinates"]["lowerLeft"] + \
                     gdal_md["cornerCoordinates"]["upperRight"]
            wgscoordinates = gdal_md["wgs84Extent"]["coordinates"][0]
            if (len(wgscoordinates) < 1):
                msg = 'Raster {} is out of bounds, no geographic bounds available.\nRaster will be deleted.\nNo metadata JSON will be created.'
                response_code = 400  # bad request
                try:
                    if os.path.isfile(output + '.tiff'):
                        os.remove(output + '.tiff')
                    if os.path.isfile(output + '.vrt'):
                        os.remove(output + '.vrt')
                    if os.path.isfile(output + '.json'):
                        os.remove(output + '.json')
                except OSError as e:
                    logging.error(e, exc_info=True)
                logging.error(
                    'Error while creating metadata json for {}.\nRaster Bounds are outside of allowed bounds.\nRaster Deleted'.format(
                        output), exc_info=True)
            else:
                geographic_bounds = wgscoordinates[1] + wgscoordinates[3]
                cog_md = {
                    "name": "{}.tiff".format(output),
                    "crs": gdal_md["coordinateSystem"]["wkt"],
                    "bounds": bounds,
                    "center": gdal_md["cornerCoordinates"]["center"],
                    "geographic_bounds": geographic_bounds
                }
                with open("{}.json".format(output), "w") as file:
                    json.dump(cog_md, file, indent=4)
                logging.info('created metadata in {}.json'.format(output))
            return msg, response_code
        except Exception as e:
            logging.error('Error while creating metadata json for {}'.format(
                output), exc_info=True)
            logging.error(e, exc_info=True)


    def create_nativ_cog(self,input, output):
        '''
        Use GDAL to build a VRT of relevant rasters, convert to .tif and create internal pyramids.
        Tests are performed before the function is run to catch possible errors.

        Args:
            input (str): input path for tiles
            output (str): path and name of output raster, without extension

        Returns:
            msg (str): result of `create_nativ_json` for success, error message for error
            response_code (int): 201 for success, 500 for error, 400 if created raster is out of globe bounds

        '''
        response_code = 201
        msg = 'Raster {} created successfully'.format(output)
        try:
            crs1 = 'PROJCRS["Israel 1993 / Israeli TM Grid",BASEGEOGCRS["Israel 1993",DATUM["Israel 1993",ELLIPSOID["GRS 1980",6378137,298.257222101,LENGTHUNIT["metre",1]]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]],ID["EPSG",4141]],CONVERSION["Israeli TM",METHOD["Transverse Mercator",ID["EPSG",9807]],PARAMETER["Latitude of natural origin",31.7343936111111,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8801]],PARAMETER["Longitude of natural origin",35.2045169444444,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8802]],PARAMETER["Scale factor at natural origin",1.0000067,SCALEUNIT["unity",1],ID["EPSG",8805]],PARAMETER["False easting",219529.584,LENGTHUNIT["metre",1],ID["EPSG",8806]],PARAMETER["False northing",626907.39,LENGTHUNIT["metre",1],ID["EPSG",8807]]],CS[Cartesian,2],AXIS["(E)",east,ORDER[1],LENGTHUNIT["metre",1]],AXIS["(N)",north,ORDER[2],LENGTHUNIT["metre",1]],USAGE[SCOPE["Cadastre, engineering survey, topographic mapping (large and medium scale)."],AREA["Israel - onshore; Palestine Territory - onshore."],BBOX[29.45,34.17,33.28,35.69]],ID["EPSG",2039]]'
            cmd1 = subprocess.Popen(["gdalbuildvrt", "-overwrite", "{}.vrt".format(
                output), "{}/*.tif".format(input), "-a_srs", "EPSG:2039"])
            logging.debug(
                "cmd1: 'gdalbuildvrt -overwrite {}.vrt {}/*.tif".format(output, input))
            cmd1.communicate()
            # cmd1 = r"gdalbuildvrt -overwrite \\gisdev01\d$\rasters\{}.vrt {}\*.tif -a_srs EPSG:2039".format(output,input)
            # cmd2 = r"gdal_translate -of COG -co COMPRESS=JPEG -co QUALITY=75 -b 1 -b 2 -b 3 \\gisdev01\d$\rasters\{}.vrt \\gisdev01\d$\rasters\{}.tif".format(output)
            cmd2 = subprocess.Popen(
                ["gdalinfo", "-json", "-nofl", "-noct", "{}.vrt".format(output)], stdout=subprocess.PIPE)
            logging.debug(
                "cmd2: 'gdalinfo -json -nofl -noct {}.vrt".format(output))
            jsonS, _ = cmd2.communicate()
            if len(json.loads(jsonS)["bands"]) < 4:
                cmd3 = subprocess.Popen(["gdal_translate", "-of", "COG", "-co", "COMPRESS=LZW",
                                         "-expand", "rgb", "{}.vrt".format(output), "{}.tiff".format(output)])
                logging.debug(
                    "cmd3: 'gdal_translate -co COMPRESS=LZW -of COG -co TARGET_SRS=EPSG:2039 -expand rgb {}.vrt {}.tiff".format(
                        output, output))
                cmd3.communicate()
            else:
                cmd3 = subprocess.Popen(["gdal_translate", "-co", "COMPRESS=LZW", "-of", "COG",
                                         "-co", "TARGET_SRS=EPSG:2039", "{}.vrt".format(output),
                                         "{}.tiff".format(output)])
                logging.debug(
                    "cmd3: 'gdal_translate -co COMPRESS=LZW -of COG -co TARGET_SRS=EPSG:2039 {}.vrt {}.tiff".format(
                        output, output))
                cmd3.communicate()
            logging.info('Nativ - Converted {} to {}'.format(input, output))
            msg, response_code = self.create_nativ_json(output)
        except Exception as e:
            logging.error('Error while converting', exc_info=True)
            logging.error(e, exc_info=True)
            response_code = 500
            msg = 'Error while converting: {}'.format(e)

        return response_code, msg


    def getNativOutputPath(self,environment, tilesDir):
        """
        Get the output path and name for the COG and metadata

        Args:
            environment (str): name of th enviroment to get the server an path
            tilesDir (str): name of the output raster


        Returns:
            outputPath (str): path and name of output raster, without extension
        """
        if environment == 'dev':
            outputPath = os.path.join(r"\\gisdevcog01\\c$\\rasters\\dev", tilesDir)
        elif environment == 'test':
            outputPath = os.path.join(r"\\gisdevcog01\\c$\\rasters\\tst", tilesDir)
        elif environment == 'ppr':
            outputPath = os.path.join(r"\\gisdevcog01\\c$\\rasters\\ppr", tilesDir)
        elif environment == 'prd':
            outputPath = os.path.join(
                r"\\gisprodcog02\\d$\\rasters\\prd", tilesDir)
        else:
            outputPath = os.path.join(r"\\gisdevcog01\\c$\\rasters\\dev", tilesDir)
        return outputPath


    def getNativRasterName(self,id, environment="test"):
        """
        For Nativ, using an ID and environment, return the name for the output raster

        Args:
            id (int) : id in the relevant database, can change by environment
            environment (str): name of the environment to be used, can be "dev", "test", "ppr" or "prod". "dev" is set by default if none is supplied (before the function is used)

        Returns:
                msg (str): name of the output raster for success, error message for error
                response_code (int): 200 for success, 500 for error
        """
        try:
            response_code = 200
            connString = self.getNativConnectionString(environment)
            conn = pyodbc.connect(connString)
            cursor = conn.cursor()
            query = self.getNativQuery(environment, "name", id)
            cursor.execute(query)
            # columns = [column[0] for column in cursor.description] # no column names, leave this here incase more columns are added
            row = cursor.fetchone()
            msg = row[0]  # raster name
        except pyodbc.DatabaseError as e:
            response_code = 500
            msg = "Database error"
        return msg, response_code


    def getNativQuery(self,environment, type, id):
        """
        Return the relevant query for the nativ databases.
        there are seperate queries for retrieving the input tiles path and the name of the output raster

        Args:
            id (int): id in the relevant database, can change by environment
            type (str): What query to return, either 'name' or 'path'
            environment (str): name of the environment to be used, can be "dev", "test", "ppr" or "prod". "dev" is set by default if none is supplied (before the function is used)

        Returns:
                query (str): query to be executed with the results of `getNativConnectionString`
        """
        try:
            if environment == 'dev':
                if type == 'name':
                    query = "EXEC db527.dbo.rb_sp_w_shetach_raster_name @prm_id_koma={};".format(
                        id)
                elif type == 'path':
                    query = "EXEC db527.dbo.rb_sp_w_shetach_raster_path;"
            elif environment == 'test':
                if type == 'name':
                    query = "EXEC db527.dbo.rb_sp_w_shetach_raster_name @prm_id_koma={};".format(
                        id)
                elif type == 'path':
                    query = "EXEC db527.dbo.rb_sp_w_shetach_raster_path;"
            elif environment == 'ppr':
                if type == 'name':
                    query = "EXEC db527.dbo.rb_sp_w_shetach_raster_name @prm_id_koma={};".format(
                        id)
                elif type == 'path':
                    query = "EXEC db527.dbo.rb_sp_w_shetach_raster_path;"
            else:
                if type == 'name':
                    query = "EXEC db527.dbo.rb_sp_w_shetach_raster_name @prm_id_koma={};".format(
                        id)
                elif type == 'path':
                    query = "EXEC db527.dbo.rb_sp_w_shetach_raster_path;"
        except Exception as e:
            query = ""
        return query


    def getNativConnectionString(self,environment='test'):
        """
        Get the connection string by the environment name
        Strings need to be recognizable in `pyodbc`, parameters split by `;`

        Args:
            environment (str): nmae of th enviroment to get the connection string for

        Returns:
            (str): A connection string to be used directly in `pyodbc.connect`
        """
        if environment == 'dev':
            connString = 'Data Source=sqldev1601;Initial Catalog=db527;Persist Security Info=True;UID=db527_d;PWD=h#885527;Driver={SQL Server};SERVER=sqldev1601;'
        elif environment == 'test':
            connString = 'Data Source=sqltst1601;Initial Catalog=db527;Persist Security Info=True;UID=db527_t;PWD=h#021527;Driver={SQL Server};SERVER=sqltst1601;'
        elif environment == 'ppr':
            connString = 'Data Source=sqlppr1601;Initial Catalog=db527;Persist Security Info=True;UID=db527_ppr;PWD=h#590527;Driver={SQL Server};SERVER=sqlppr1601;'
        elif environment == 'prd':
            connString = 'Data Source=sqlprd1601ag4ls;Initial Catalog=db527;Persist Security Info=True;UID=db527_p;PWD=h#527560;Driver={SQL Server};SERVER=sqlprd1601ag4ls;'
        else:
            connString = 'Data Source=sqltst1601;Initial Catalog=db527;Persist Security Info=True;UID=db527_t;PWD=h#021527;Driver={SQL Server};SERVER=sqltst1601;'
        return connString
    def getRasterTilesPath(self,id, environment="test"):
        """
        For Nativ, using an ID and environment, return the path of the input tiles

        Args:
            id (int) : id in the relevant database, can change by environment
            environment (str): name of the environment to be used, can be "dev", "test", "ppr" or "prod". "dev" is set by default if none is supplied (before the function is used)

        Returns:
                msg (str): path of the input tiles for success, error message for error
                response_code (int): 200 for success, 500 for error
        """
        try:
            connString = self.getNativConnectionString(environment)
            query = self.getNativQuery(environment, "path", id)
            conn = pyodbc.connect(connString)
            cursor = conn.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            response_code = 200
            msg = row[0]  # raster path
        except pyodbc.DatabaseError as e:
            response_code = 500
            msg = "Database error: {}".format(e)
            logging.error(msg)
        except Exception as e:
            response_code = 500
            msg = "Error while getting raster tiles path: {}".format(e)
            logging.error("Error while getting raster tiles path", exc_info=True)
            logging.error(e, exc_info=True)
        return msg, response_code
        
        
    def copy_rishui(self,id_koma, environment):
        """
        API endpoint for creating nativ files.
        nativ files have a unique id for retreiving the name of the vrt, tif (COG) and JSON files without their extension.
        ---
        Args:
            id_koma (int): id for retreiving input location and output name with `getRasterTilesPath` and `getNativRasterName`
            env (str): working environment, determines which management server and tables are queried

        Responses:
            (Object): JSON response and a response code:
            \n\t\t500: Description: Error, description added.
            \n\t\t400: (Bad Request): bad request, missing id_koma
            \n\t\t201: (Success): Files created successfully

            JSON response for errors is always:\n
                {"error": The Error Message}

            JSON response for bad requsets is:\n
                {'Bad Request': 'missing id_koma'}

            JSON response for success is:\n
                {'msg': 'created metadata in {}.json',
                'tilejson': URL of the output raster's metadata file}

        """
        response_code = 201
        msg = "Created Succefully"
        try:
            logging.info('nativ started id_koma: {}'.format(id_koma))
            if id_koma is None:
                response_code = 400
                response_object = {
                    'Bad Request': 'missing id_koma'
                }

                return response_object, response_code
            basePath, status = self.getRasterTilesPath(id_koma, environment)
            if status != 200:
                response_code = status
                response_object = {
                    'error': basePath
                }

                return response_object, response_code
            tilesDir, status = self.getNativRasterName(id_koma, environment)
            if status != 200:
                response_code = status
                response_object = {
                    'error': tilesDir
                }

                return response_object, response_code
            inputPath = os.path.join(basePath, tilesDir)
            outputPath = self.getNativOutputPath(environment, tilesDir)
            numTifs = len([name for name in os.listdir(
                inputPath) if name.endswith('.tif')])
            if numTifs <= 0:
                logging.error(
                    "No tif images in the input folder, '{}' is empty".format(inputPath))
                logging.error("failed to create raster for env: {}, id_koma: {}".format(
                    environment, id_koma))
                response_code = 500
                msg = "No tif images in the input folder, '{}' is empty".format(
                    inputPath)
                response_object = {
                    "message": msg
                }

                return response_object, response_code
            status, msg = self.create_nativ_cog(inputPath, outputPath)
            response_code = status
            response_object = {
                'msg': msg,
                'tilejson': self.getTileJsonAddress(environment, tilesDir)
            }
            if response_code != 201:
                response_object = {
                    'msg': msg
                }

        except Exception as e:
            logging.error(e, exc_info=True)
            response_code = 500
            response_object = {
                'error': e
            }

        return response_object, response_code


    def checkTempKey(self,key):
        """
        Check if the key is valid and not expired
        Checks all files even though the files are per user.

        Args:
            key (str): User specific temporary key created (and returned to the user) with `setTempKey`

        Returns:
            msg (str): "Authorized" for success, 'Key expired, generate new key', 'Unknown key', 'Bad request' for bad requests, error message for error
            response_code (int): 200 for success, 500 for error, 400 for bad request, 401 (unauthorized) for invalid or expired keys
        """
        try:
            # file = os.path.join(dirname, 'token') # commented out to support multiple files
            for file in os.listdir(self.dirname):
                if file.endswith('.token'):
                    with open(os.path.join(self.dirname, file), 'r') as openfile:
                        json_object = json.load(openfile)
                        now = dt.now()
                        known_key = json_object['key']
                        expires = dt.fromtimestamp(json_object['expires'])
                        if known_key == key and expires > now:
                            response_code = 200
                            msg = 'Authorized'
                            break
                        elif known_key == key and now > expires:
                            msg = 'Key expired, generate new key'
                            response_code = 401
                            break
                        elif known_key != key:
                            msg = 'Unknown key'
                            response_code = 401
                        else:
                            msg = 'Bad request'
                            response_code = 400
        except Exception as e:
            logging.error(e, exc_info=True)
            msg = 'Error'
            response_code = 500
        return msg, response_code
        
        
    def nativDeleteRaster(self,id_koma, environment, key):
        """
        API endpoint for deleting nativ files.
        nativ files have a unique id for retreiving the name of the vrt, tif (COG) and JSON files without their extension.
        ---
        Args:
            id_koma (int): id of the files to be deleted
            env (str): working environment, determines which management server and tables are queried
            key (str): The user temporary key generated using the `verifyNativ` function of the `nativ_token` app route.

        Responses:
            (Object): JSON response and a response code:
            \n\t\t500: Description: Error, description added.
            \n\t\t400: (Bad Request): bad request, missing key or id_koma
            \n\t\t204: (Success): Files deleted successfully

            JSON response for errors is always:\n
                {"error": The Error Message}

            JSON response for bad requsets is:\n
                {'Bad Request': 'missing key/id_koma'}

            JSON response for success is:\n
                {'msg': 'Removed Succesfully'}

        """
        response_code = 204
        msg = "Deleted Succefully"
        try:
            if key is None:
                response_code = 400
                response_object = {
                    'Bad Request': 'missing key'
                }
                return response_object, response_code
            if id_koma is None:
                response_code = 400
                response_object = {
                    'Bad Request': 'missing id_koma'
                }

                return response_object, response_code
            else:
                msg, status = self.checkTempKey(key)
                if status != 200:
                    response_code = status
                    response_object = {
                        'error': msg
                    }

                    return response_object, response_code
            tilesDir, status = self.getNativRasterName(id_koma, environment)
            if status != 200:
                response_code = status
                response_object = {
                    'error': tilesDir
                }

                return response_object, response_code
            rasterPath = self.getNativOutputPath(environment, tilesDir)
            try:
                if os.path.isfile(rasterPath + '.tiff'):
                    os.remove(rasterPath + '.tiff')
                if os.path.isfile(rasterPath + '.vrt'):
                    os.remove(rasterPath + '.vrt')
                if os.path.isfile(rasterPath + '.json'):
                    os.remove(rasterPath + '.json')
                response_object = {
                    'msg': 'Removed Succesfully'
                }
                response_code = 200
            except OSError as e:
                logging.error(e, exc_info=True)

        except Exception as e:
            logging.error(e, exc_info=True)
            response_code = 500
            response_object = {
                'error': e
            }

        return response_object, response_code


    def setTempKey(self,id):
        """
        Create a temporary key for raster deletions, writes the key to file and returns it.
        Key is stored in JSON format in a file which will be named: `{user_id}.token` (exmaple: 1.token, 2.token etc.)
        The JSON is in the following structure:

            {"key": "1cec9cd3a758", "expires": 1671363341}

        A unique temporary key and an expiry timestamp.

        Args:
            id (int): id of the user in the users file

        Returns:
            temp_dict (str): string of a JSON object with the key and its expiry timestamp
        """
        key = secrets.token_hex(6)
        expires = dt.now() + timedelta(minutes=5)
        expires_str = round(expires.timestamp())
        temp_dict = json.dumps({'key': key, 'expires': expires_str})
        file = os.path.join(self.dirname, '{}.token'.format(id))
        with open(file, "w") as outfile:
            outfile.write(temp_dict)
        return temp_dict


    def checkCredentials(self,user, password):
        """
        Check if the key is valid and not expired
        Checks all files even though the files are per user.

        Args:
            key (str): User specific temporary key created (and returned to the user) with `setTempKey`

        Returns:
            msg (str): "Authorized" for success, 'Key expired, generate new key', 'Unknown key', 'Bad request' for bad requests, error message for error
            response_code (int): 200 for success, 500 for error, 400 for bad request, 401 (unauthorized) for invalid or expired keys
        """
        try:
            file = os.path.join(self.dirname, 'users')
            password_encoded = base64.b64encode(password.encode("utf-8"))
            with open(file, 'r') as openfile:
                json_object = json.load(openfile)
                for user_obj in json_object['users']:
                    if user == user_obj['name']:
                        user_id = str(user_obj['id'])
                        known_password = json_object['passwords'][user_id]['password'].encode(
                            'utf-8')
                        if known_password == password_encoded:
                            response_code = 200
                            response_object = self.setTempKey(user_id)
                        else:
                            response_code = 401
                            response_object = {
                                'msg': 'Bad Password'
                            }
        except Exception as e:
            logging.error(e, exc_info=True)
            response_code = 500
            response_object = {
                'error': e
            }
        return response_object, response_code
        
        
    def verifyNativ(self,user, password):
        """
        API endpoint for verifying deletion priviliges
        should return a JSON response with a key and its expiry timestamp
        ---
        Args:
            user (str): user name, should be included in `users` file
            password (str): user password, should match one in `users` file

        Responses:
            (Object): JSON response and a response code:
            \n\t\t500: Description: Error, description added.
            \n\t\t400: (Bad Request): bad request, missing user or password
            \n\t\t401: (Unauthorized): Bad Password
            \n\t\t200: (Success): User exists, password verified, token created

            JSON response for errors is always:\n
                {"error": The Error Message}

            JSON response for bad requsets is:\n
                {'msg': 'Bad Request'}

            JSON response for success is:\n
                {"key": "string represeting current temporary key", "expires": int representing timestamp of key expiration}

        """
        try:

            if None in (user, password):
                response_object = {
                    'msg': 'Bad Request'
                }
                response_code = 400
                return response_object, response_code

            response_object, response_code = self.checkCredentials(user, password)

        except Exception as e:
            logging.error(e, exc_info=True)
            response_object = {
                'msg': 'Server Error'
            }
            response_code = 500

        return response_object, response_code
