#!/usr/bin/python

import csv
import hashlib
import os
import shutil
import sys
import time

def getFileHash(path):
    with open(path, 'rb') as f:
        data=f.read()

    hash=hashlib.sha1("blob {0}\0{1}".format(len(data), data))
    return hash.hexdigest()

def getFiles(path):
        for root, subFolders, files in os.walk(path):
            for file in files:
                yield os.path.join(root, file)

class DocumentHandler:

    def __init__(self, documentList = []):
        self.documentList = documentList
        self.documentMap = {}
        for document in documentList:
            self.documentMap[document['file_path']] = document

    def getFieldnames(self):
        return ['file_path', 'mtime', 'sha1']

    def create(self, file_path, mtime, sha1 = None):
        return { 'file_path': file_path, 'mtime': mtime, 'sha1': sha1 }

    def setSha1(self, document, sha1):
        document['sha1'] = sha1

    def getFromList(self, document):
        if document['file_path'] in self.documentMap:
            return self.documentMap[document['file_path']]
        return None

    def appendToList(self, document):
        document_ref = document.copy()
        self.documentList.append(document_ref)
        self.documentMap[document_ref['file_path']] = document_ref

    def replaceToList(self, document):
        document_old_ref = self.getFromList(document)
        document_ref = document.copy()
        self.documentList.remove(document_old_ref)
        self.documentList.append(document_ref)
        self.documentMap[document_ref['file_path']] = document_ref

    def removeToList(self, document):
        document_old_ref = self.getFromList(document)
        self.documentList.remove(document_old_ref)
        del self.documentMap[document_old_ref['file_path']]

    def newerThan(self, document1, document2):
        return document1['mtime'] > document2['mtime']

def createIndexIfNotExists(documentHandler):
    if os.path.isfile('.watcher/index') == False:
        with open('.watcher/index', 'wb') as csvfile:
            fieldnames = documentHandler.getFieldnames()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()


def loadIndex():
    document_list = []
    with open('.watcher/index', 'rb') as csvfile:
        document_iterator = csv.DictReader(csvfile)
        for document_raw in document_iterator:
            document_raw['mtime'] = float(document_raw['mtime'])
            document_list.append(document_raw)


    return document_list

def writeIndex(documentHandler, documentList):
    try:
        # As we rewrite the index entirely if nothing went wrong
        # like a user interruption (CTRL + C or kill) during the process,
        # we lose the indexing file
        #
        # It writes index file in tmp file and replace the existing one
        # at the end. As move operation is atomic, no risk losing the
        # index file.
        #
        # http://stackoverflow.com/questions/3716325/is-pythons-shutil-move-atomic-on-linux
        with open('.watcher/index~', 'wb') as csvfile:
            fieldnames = fieldnames = documentHandler.getFieldnames()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for document in documentList:
                writer.writerow(document)

        shutil.move('.watcher/index~', '.watcher/index')
    finally:
        if os.path.isfile('.watcher/index~'):
            os.remove('.watcher/index~')

def main(args):
    initialDocumentHandler = DocumentHandler()
    createIndexIfNotExists(initialDocumentHandler)
    while True:
        print 'start indexing'
        counter_change = 0
        counter_crawl = 0
        document_list = loadIndex()
        documentHandler = DocumentHandler(document_list)
        try:
            document_to_delete = list(document_list)
            for file_path in getFiles(args[1]):
                try:
                    counter_crawl += 1
                    lastdate = os.stat(file_path).st_mtime
                    document = documentHandler.create(file_path, lastdate, None)
                    document_ref = documentHandler.getFromList(document)
                    if document_ref != None:
                        document_to_delete.remove(document_ref)

                    if document_ref == None:
                        counter_change += 1
                        sha1 = getFileHash(file_path)
                        documentHandler.setSha1(document, sha1)
                        documentHandler.appendToList(document)
                        print 'append document in index {0}'.format(document)
                    elif documentHandler.newerThan(document, document_ref):
                        counter_change += 1
                        sha1 = getFileHash(file_path)
                        documentHandler.setSha1(document, sha1)
                        documentHandler.replaceToList(document)
                        print 'replace document in index {0} by {1}'.format(document_ref, document)
                except (OSError, IOError):
                    pass

                # Write index every 200 changes, it improve resiliency and
                # avoid losing all information if a long indexing is in progress
                # and the script stops
                if counter_change != 0 and (counter_change % 200) == 0:
                    print('write index {0}'.format(counter_change))
                    writeIndex(documentHandler, document_list)

                if counter_crawl != 0 and (counter_crawl % 500) == 0:
                    print('file crawled {0}'.format(counter_crawl))


            for document in document_to_delete:
                documentHandler.removeToList(document)
                print 'remove document in index {0}'.format(document)

        finally:
            writeIndex(documentHandler, document_list)

        print 'end indexing'
        time.sleep(5)


if __name__ == "__main__":
    main(sys.argv)
