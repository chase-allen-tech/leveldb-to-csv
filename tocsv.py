import os
import io
import json
import sys
import re
import csv

# command-line arguments
backupFolder = os.path.normpath(sys.argv[1])

# os.chdir('firestore-leveldb-tools')
# backupFolder = os.path.normpath('./src')

repoRoot = os.path.dirname(os.path.realpath(__file__))

# import google sdks
sys.path.append(os.path.join(repoRoot, 'SDKs/google_appengine'))
sys.path.append(os.path.join(repoRoot, 'SDKs/google-cloud-sdk/lib/third_party'))
from google.appengine.api.files import records
from google.appengine.datastore import entity_pb
from google.appengine.api import datastore

def GetCollectionInJSONTreeForProtoEntity(jsonTree, entity_proto):
  result = jsonTree
  for element in entity_proto.key().path().element_list():
    nextKey = None
    if element.has_type(): nextKey = element.type()
    elif element.has_name(): nextKey = element.name()
    if nextKey is not None:
      if nextKey not in result:
        result[nextKey] = {}
      result = result[nextKey]
  return result

def GetKeyOfProtoEntity(entity_proto):
  for element in reversed(entity_proto.key().path().element_list()):
    if element.has_name(): return element.name()

def GetValueOfProtoEntity(entity_proto):
  return datastore.Entity.FromPb(entity_proto)

def Start():
  jsonTree = {}
  items = []

  for filename in os.listdir(backupFolder):
    if not filename.startswith("output-"): continue
    print("Reading from:" + filename)
    
    inPath = os.path.join(backupFolder, filename)
    raw = open(inPath, 'rb')
    reader = records.RecordsReader(raw)
    for recordIndex, record in enumerate(reader):
      entity_proto = entity_pb.EntityProto(contents=record)
      collectionInJSONTree = GetCollectionInJSONTreeForProtoEntity(jsonTree, entity_proto)
      key = GetKeyOfProtoEntity(entity_proto)
      entity = GetValueOfProtoEntity(entity_proto)
      collectionInJSONTree[key] = entity
      items.append(entity) # also add to flat list, so we know the total item count
      print("Parsing document #" + str(len(items)))
      
  outPath = os.path.join(backupFolder, 'Data.json')
  csvOutPath = os.path.join(backupFolder, 'output.csv')


  out = open(outPath, 'w')
  out.write(json.dumps(jsonTree, default=JsonSerializeFunc, encoding='latin-1', indent=2))
  out.close()

  proc(outPath, csvOutPath)

  print("JSON file written to: " + outPath)
  print("CSV file written to: " + csvOutPath)

def JsonSerializeFunc(obj):
  import calendar, datetime

  if isinstance(obj, datetime.datetime):
    if obj.utcoffset() is not None:
      obj = obj - obj.utcoffset()
    millis = int(
      calendar.timegm(obj.timetuple()) * 1000 +
      obj.microsecond / 1000
    )
    return millis
  #raise TypeError('Not sure how to serialize %s' % (obj,))
  return str(obj)

def readJsonFile(file_name):
    with open(file_name) as f:
        d = json.load(f)
        f.close()
        return d

def clearOutputFile(file_name):
    f = open(file_name, 'w+')
    f.truncate(0)
    f.close()

def writeOutput(file_name, data):
    with open(file_name, 'ab') as f:
        writer = csv.writer(f)
        writer.writerow(data)

def writeHeader(file_name, data):
    with open(file_name, 'a+') as f:
        writer = csv.writer(f)

def getFilteredString(v):
    return re.sub(r'[^\x20-\x7e]', '', v.encode('ascii', errors='ignore')) if hasattr(v, 'encode') else v

def proc(INPUT_FILE, OUTPUT_FILE):
    # INPUT_FILE = './src/Data1.json'
    # OUTPUT_FILE = './src/output.csv'

    json_data = readJsonFile(INPUT_FILE)

    header = []
    contents = []
    for document_k, document_v in json_data.items():
        for row_k, row_k in document_v.items():
            row = ['' for i in range(len(header))]
            for k, v in row_k.items():
                ch = getFilteredString(v)

                if isinstance(ch, list):
                    ch_temp = "["
                    for item in ch:
                        ch_temp += getFilteredString(item) + ", "
                    ch = ch_temp + "]"

                header_index = header.index(k) if k in header else -1
                if header_index < 0:
                    header_index = len(header)
                    header.append(k)
                    row.append(ch)
                else:
                    row[header_index] = ch
            contents.append(row)

    clearOutputFile(OUTPUT_FILE)
    writeOutput(OUTPUT_FILE, header)
    for content in contents:
        writeOutput(OUTPUT_FILE, content)


Start()