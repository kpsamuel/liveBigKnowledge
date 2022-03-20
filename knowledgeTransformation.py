# -*- coding: utf-8 -*-
"""
Created on Sun Feb 27 20:12:48 2022

@author: samuel
"""
import os, sys
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
import numpy as np


import dbUtil
dbops = dbUtil.databaseConnect()


class liveCountVector():
    
    def __init__(self):
        """
            live Count vectorizer.
            its unlike the general tfidf vectorizer which needs all the documents in hand to calculate the vector representation.
            
            live-count will keep track of all the required parameters in db and in runtime variables.
            and keeps updating db based on new document gets transformed into vector.
            
        Returns
        -------
        None.

        """
        self.global_vocabulary = {}
        self.first_time_creation = -1
        self.loadLearntVectorizer()
    
    def createVectorizer(self):
        
        self.vectorizer = CountVectorizer()
        self.vectorizer.vocabulary_ = self.global_vocabulary
        
    def loadLearntVectorizer(self):
        """
            load the vocab from the database.
            if its for the first time, will be used default self.global_vocabulary

        Returns
        -------
        None.

        """
        try:
            query = {"collection_name" : "countVectorRepresentation",
                     "select_query" : {}}
            
            db_vocab, _ = dbops.getData(query)
            if len(db_vocab) != 0:
                self.global_vocabulary = db_vocab[0]
                
        except Exception as ecp:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            error = f"{exc_type} : {fname} at {exc_tb.tb_lineno}: msg : {ecp}"
            print(error)
            
    def pushLearntVectorizer(self):
        """
            insert / update the new vocab generate to database

        Returns
        -------
        None.

        """
        try:        
            
            
            query = {"collection_name" : "countVectorRepresentation",
                     "data" : {}}
            
            if self.first_time_creation == 0:                
                query["data"] = self.global_vocabulary
                dbops.insertData(query)
                self.first_time_creation = -1
            else:
                query["data"] = self.new_vocab
                dbops.updateData(query)
            
        except Exception as ecp:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            error = f"{exc_type} : {fname} at {exc_tb.tb_lineno}: msg : {ecp}"
            print(error)

        
    def updateGlobalVectorizer(self, doc):
        """
            call this function to generate the new vocab for the input doc

        Parameters
        ----------
        doc : str
            input document to get the vocab as per vectorizer setup

        Returns
        -------
        None.

        """
        try:
            if isinstance(doc, list) == False:
                doc = [doc]
                
            vectorizer = CountVectorizer()
            vectorizer.fit(doc)
            doc_vocab = vectorizer.vocabulary_.keys()
            new_words = set(doc_vocab) - set(self.global_vocabulary)
            
            
            if '_id' in self.global_vocabulary.keys():
                self.global_vocabulary.pop('_id')
            
            if len(self.global_vocabulary) == 0:
                self.global_vocabulary = vectorizer.vocabulary_
                self.first_time_creation = 0
                self.pushLearntVectorizer()    
            
                
            elif len(new_words) > 0:
                current_vocab_count = len(self.global_vocabulary) - 1
                self.new_vocab = {word : word_index+current_vocab_count+1 for word_index, word in enumerate(new_words)}
                self.global_vocabulary.update(self.new_vocab)
                self.pushLearntVectorizer()    
                
                
            
        except Exception as ecp:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            error = f"{exc_type} : {fname} at {exc_tb.tb_lineno}: msg : {ecp}"
            print(error)

        
        
class liveTFIDFVector():
    
    def __init__(self):
        """
            live TFIDF vectorizer.
            its unlike the general tfidf vectorizer which needs all the documents in hand to calculate the vector representation.
            
            live-tfidf will keep track of all the required parameters in db and in runtime variables.
            and keeps updating db based on new document gets transformed into vector.
            
        Returns
        -------
        None.

        """
        self.global_vocabulary = {}
        self.number_global_document = 0  
        self.word_document_count = {}
        self.first_time_creation = -1
        
        self.loadLearntVectorizer()
    
    def createVectorizer(self):
        
        self.vectorizer = TfidfVectorizer()
        self.vectorizer.vocabulary_ = self.global_vocabulary
        
    def loadLearntVectorizer(self):
        """
            load the vocab from the database.
            if its for the first time, will be used default self.global_vocabulary

        Returns
        -------
        None.

        """
        try:
            ## loading the learnt vector representation
            query = {"collection_name" : "tfidfVectorRepresentation",
                     "select_query" : {}}
            
            ## loading the vector / vocab metadata
            db_vocab, _ = dbops.getData(query)
            if len(db_vocab) != 0:
                self.global_vocabulary = db_vocab[0]["representation"]
                self.number_global_document = db_vocab[0]["number_global_document"]
                self.word_document_count = db_vocab[0]["word_document_count"]
                self.first_time_creation = 0
                
        except Exception as ecp:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            error = f"{exc_type} : {fname} at {exc_tb.tb_lineno}: msg : {ecp}"
            print(error)

    
    def pushLearntVectorizer(self):
        """
            insert / update the new vocab generate to database

        Returns
        -------
        None.

        """
        try:        
            
            
            query = {"collection_name" : "tfidfVectorRepresentation",
                     "data" : {"representation" : self.global_vocabulary,
                               "number_global_document" : self.number_global_document,
                               "word_document_count" : self.word_document_count}}
            
            if self.first_time_creation == -1:                
                dbops.insertData(query)
                self.first_time_creation = 0
            else:
                query["data"]["representation"] = self.global_vocabulary
                dbops.updateData(query)
                
        except Exception as ecp:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            error = f"{exc_type} : {fname} at {exc_tb.tb_lineno}: msg : {ecp}"
            print(error)


    def termFrequency(self, doc, doc_vocab):
        """
            function will get the count of the word in the given doc.
            if the word present in the global document word it will increment the count or else
            it will create a new key and put the count

        Parameters
        ----------
        doc : list
            list of documents
        doc_vocab : dict
            words needs to be searched and its count

        Returns
        -------
        term_frequecy : dict
            each word of vocab as key and its count in value

        """
        term_frequecy = {}
        for doc_instance in doc:
            for word in doc_vocab:
                term_frequecy[word] = doc_instance.count(word)
                
                if word not in self.word_document_count.keys():
                    self.word_document_count[word] = 1
                else:
                    self.word_document_count[word] += term_frequecy[word]
                    
        return term_frequecy
    
    
    def inverseTermFrequency(self, doc_vocab):
        """
            formula used
                itf = N / (Dw + 1)
                      N : number of documents present at global level
                      Dw : number of document have word W

        Parameters
        ----------
        doc_vocab : dict
            words dict whose itf needs to be calculated

        Returns
        -------
        inverse_term_frequency : dict
            inverse term frequency of each word from the doc_vocab

        """
        inverse_term_frequency = {word : self.number_global_document / (self.word_document_count[word]+1) for word in doc_vocab}
        return inverse_term_frequency
        
    def calculateTFIDF(self, doc, doc_vocab):
        """
            direct tfidf calculation with frequently changing documents. 
            updates the global vocab and other indexing for further calculation.

        Parameters
        ----------
        doc : str
            sentence
        doc_vocab : dict
            word from doc

        Returns
        -------
        None.

        """
        tf = self.termFrequency(doc, doc_vocab)
        idf = self.inverseTermFrequency(doc_vocab)
        tf_idf = {word : (tf[word] * np.log(idf[word]))+1 for word in doc_vocab}
        
        ## updating the global vector representation for doc_vocab words
        for word in doc_vocab:
            if word in self.global_vocabulary.keys():
                self.global_vocabulary[word] += tf_idf[word]  
            else:
                self.global_vocabulary[word] = tf_idf[word]
            
        
        
    def updateGlobalVectorizer(self, doc):
        """
            function will be used to calculate the ifidf in Runtime mode, 
            unlike general tfidf which needs all document in place to get the calculation done.

            this function automatically updates the new tfidf values into db and keeps track of other parameterd required for tfidf calculation
        Parameters
        ----------
        doc : str
            sentencce

        Returns
        -------
        None.

        """
        ## check the input doc type and increment the global document number
        if isinstance(doc, list) == False:
            doc = [doc]
            self.number_global_document += len(doc)
        else:            
            self.number_global_document += 1
        
        if '_id' in self.global_vocabulary.keys():
            self.global_vocabulary.pop('_id')
            
        vectorizer = TfidfVectorizer()
        vectorizer.fit(doc)
        doc_vocab = vectorizer.vocabulary_.keys()
        self.calculateTFIDF(doc, doc_vocab)
        self.new_vocab = {word : self.global_vocabulary[word] for word in doc_vocab}
        
        self.pushLearntVectorizer()
        