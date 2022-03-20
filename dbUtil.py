# -*- coding: utf-8 -*-
"""
Created on Sun Jun 13 10:14:38 2021

@author: samuel
"""
import json
import os
from pymongo.mongo_client import MongoClient


with open("config.json", "r") as file:
    configData = json.load(file)
    
    
if configData["backupDataFolder"] not in os.listdir():
    os.mkdir(configData["backupDataFolder"])
    
class databaseConnect():
    
    def __init__(self):
        """
            making connectiong to mongo DB and connecting to database. 
            All required information of database hostname, port number and database name are read from config.json file
        """
        try:
            self.client = MongoClient(host=configData["dbHostName"], port=configData["dbPortNumber"])
            self.db = self.client[configData["dbName"]]
            self.db_connection_status = 0
        except Exception as err_msg:
            self.db_connection_status = -1
            print("error :",configData["errorID"]["500"] + " : "+str(err_msg))
            
            
    def insertData(self, query):
        """
            method to insert data into any collection the required format is as per belw examples. based on type of "data" insert operation is performed

            input : query (dict)
            example : query = {"collection_name" : "abc", "data" : {}}   
                      query = {"collection_name" : "abc", "data" : []} 
            
            NOTE : in case insert operation fails, than it will automatically moves to write operation to json file, the file name will be collection name.

        """
        if self.db_connection_status == 0:
            try:
                ## connecting to db-collection
                db_collection = self.db[query["collection_name"]]
                
                ## checking if "data" key type is "dict" (operation of single data record performed)
                if isinstance(query["data"], dict):
                    db_collection.insert_one(query["data"])

                ## checking if "data" key type is "list" (operation of multi data records performed)
                if isinstance(query["data"], list):
                    db_collection.insert_many(query["data"])
                    
            except Exception as err_msg:
                
                print("error :",configData["errorID"]["501"] + " : "+str(err_msg))

                ## calling data write to json file, in case of db-insert operation is failed.
                self.writeData(query)
                
    def updateData(self, query):
        
        try:
            db_collection = self.db[query["collection_name"]]
            filter_query = {}
            new_data = {"$set" : query["data"]}
            
            if isinstance(query["data"], dict):
                db_collection.update_one(filter_query, new_data)
                
            if isinstance(query["data"], list):
                db_collection.update_many(filter_query, new_data)
                
        except Exception as err_msg:
            print("error :", configData["errorID"]["505"] + " : "+str(err_msg))
            print(query)
            
    
    def writeData(self, query):
        """
            writing data to json file, file name will be same as collection_name key value.
            This is append operation so all failed records are stored in those files.

            NOTE : all the json's will be saved in "backupDataFolder" key value folder, 
            which you can set it from config.json file, by default its value is "dataBackup".
        """
        try:
            backup_savepath = os.path.join(configData["backupDataFolder"], query["collection_name"]) + ".json"
            with open(backup_savepath, "w+") as fp:
                json.dump(query["data"], fp)
        except Exception as err_msg:
            print("error :",configData["error"]["502"] + " : "+str(err_msg))

        
    def getData(self, query):
        """
            general method to make select operation from any collection using "select_query".
            example : 
                    query = {"collection_name" : "abc", "select_query" : {"mmid" : "oopp-0920-2ek"}}
        """
        try:
            db_collection = self.db[query["collection_name"]]
            data = [log for log in db_collection.find(query["select_query"], {"_id":False})]
            return data, 0
        except Exception as err_msg:
            print("error :",configData["errorID"]["504"] + " : "+str(err_msg))
            return [], -1
        
        
            
    def deleteRecords(self, query):
        """
            general method to make delete operation from any collection using "delete_query".
            example : 
                    query = {"collection_name" : "abc", "delete_query" : {"mmid" : "oopp-0920-2ek"}}
        """
        try:
            if self.db_connection_status == 0:
                db_collection = self.db[query["collection_name"]]
                db_collection.remove(query["delete_query"])
                return 0
            else:
                return -1
        except Exception as err_msg:
            print("error : "+configData["errorID"]["503"] + " : "+str(err_msg))
        
        