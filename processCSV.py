## Accompanying code for - Processing large S3 files with AWS Lambda
## https://medium.com/swlh/processing-large-s3-files-with-aws-lambda-2c5840ae5c91

import urllib.request
import csv
import json
import os

import boto3
import botocore.response


from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED

MINIMUN_REMAINING_TIME_MS   = 10000
REGION                      = "us-east-1"
KEYSPACES_HOST              = "cassandra.us-east-1.amazonaws.com"
KEYSPACES_PORT              = 9142
KEYSPACES_CERT_URL          = "https://certs.secureserver.net/repository/sf-class2-root.crt"
KEYSPACES_CERT_PATH         = "/tmp/sf-class2-root.crt"
KEYSPACES_NAME              = "orats"
KEYSPACES_TABLE             = "snapshots"

columns = ["\"ticker\"",
	"\"tradeDate\"",
	"\"expirDate\"",
	"\"dte\"",
	"\"strike\"",
	"\"stockPrice\"",
	"\"callVolume\"",
	"\"callOpenInterest\"",
	"\"callBidSize\"",
	"\"callAskSize\"",
	"\"putVolume\"",
	"\"putOpenInterest\"",
	"\"putBidSize\"",
	"\"putAskSize\"",
	"\"callBidPrice\"",
	"\"callValue\"",
	"\"callAskPrice\"",
	"\"putBidPrice\"",
	"\"putValue\"",
	"\"putAskPrice\"",
	"\"callBidIv\"",
	"\"callMidIv\"",
	"\"callAskIv\"",
	"\"smvVol\"",
	"\"putBidIv\"",
	"\"putMidIv\"",
	"\"putAskIv\"",
	"\"residualRate\"",
	"\"delta\"",
	"\"gamma\"",
	"\"theta\"",
	"\"vega\"",
	"\"rho\"",
	"\"phi\"",
	"\"driftlessTheta\"",
	"\"callSmvVol\"",
	"\"putSmvVol\"",
	"\"extSmvVol\"",
	"\"extCallValue\"",
	"\"extPutValue\"",
	"\"spotPrice\"",
	"\"quoteDate\"",
	"\"updatedAt\"",
	"\"snapShotEstTime\"",
	"\"snapShotDate\"",
	"\"expiryTod\"",
]

def build_query():
    insert_query = insert_query + "INSERT INTO " + KEYSPACES_NAME + "." + KEYSPACES_TABLE
    insert_query = insert_query + " ("
    for c in range(columns) :
    	insert_query = insert_query + c + ", "

    insert_query = insert_query[:len(insert_query)-2]
    insert_query = insert_query + ")" + " values ("
    for i in range(len(columnsTypes)) :
        insert_query = insert_query + '?' + ", "
        
    insert_query = insert_query[:len(insert_query)-2]
    insert_query = insert_query + ")"

def lambda_handler(event, context):
    print(event)
    
    print("Downloading ", KEYSPACES_CERT_URL)
    urllib.request.urlretrieve(KEYSPACES_CERT_URL, KEYSPACES_CERT_PATH)
    keyspaces_user = os.environ.get('KEYSPACES_USER')
    keyspaces_pass = os.environ.get('KEYSPACES_PASS')
    
    records = [x for x in event.get('Records', [])]
    sorted_events = sorted(records, key=lambda e: e.get('eventTime'))
    latest_event = sorted_events[-1] if sorted_events else {}
    info = latest_event.get('s3', {})
    object_key = info.get('object', {}).get('key')
    bucket_name = info.get('bucket', {}).get('name')
    offset = event.get('offset', 0)
    row_count = event.get('row_count', 0)
    fieldnames = event.get('fieldnames', None)
    print("Field Names:", fieldnames)
    print("Row Count:", row_count)
    print("Offset:", offset)

    access_key = os.environ.get('ACCESS_KEY')
    secret_key = os.environ.get('SECRET_KEY')
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)
    
    print("Accessing S3 bucket", bucket_name, object_key)
    s3_resource = session.resource('s3')
    s3_object = s3_resource.Object(bucket_name=bucket_name, key=object_key)
    bodylines = get_object_bodylines(s3_object, offset)
    csv_reader = csv.DictReader(bodylines.iter_lines(), fieldnames=fieldnames)
    test = 1
    for row in csv_reader:
        row_count += 1
        
        if test == 1:
            print("First Row:", row)
            test += 1
            
        ## process and do work
        
        ssl_context = SSLContext(PROTOCOL_TLSv1_2 )
        ssl_context.load_verify_locations(KEYSPACES_CERT_PATH)
        ssl_context.verify_mode = CERT_REQUIRED
        auth_provider = PlainTextAuthProvider(username=keyspaces_user, password=keyspaces_pass)
        cluster = Cluster([KEYSPACES_HOST], ssl_context=ssl_context, auth_provider=auth_provider, port=KEYSPACES_HOST)
        session = cluster.connect()
        return
        ## end work
        
        
        if context.get_remaining_time_in_millis() < MINIMUN_REMAINING_TIME_MS:
            fieldnames = fieldnames or csv_reader.fieldnames
            print("Last Row:", row)
            break

    print("Processed %d rows" % row_count)
    
    new_offset = offset + bodylines.offset
    if new_offset < s3_object.content_length:
        print("Invoke next call with offset ", new_offset)
#        new_event = {
#            **event,
#            "offset": new_offset,
#            "fieldnames": fieldnames,
#            "row_count": row_count
#        }
#        invoke_lambda(context.function_name, new_event)
    else:
        print("All done processing", bucket_name, object_key)
        print("Total Rows: ", row_count)
    return


def invoke_lambda(function_name, event):
    access_key = os.environ.get('ACCESS_KEY')
    secret_key = os.environ.get('SECRET_KEY')
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)
        
    client = session.client('lambda')
    payload = json.dumps(event).encode('utf-8')
    
    response = client.invoke(
        FunctionName=function_name,
        InvocationType='Event',
        Payload=payload
    )


def get_object_bodylines(s3_object, offset):
#    resp = s3_object.get(Range=f'bytes={offset}-')
    body: botocore.response.StreamingBody = resp['Body']
    return BodyLines(body)


class BodyLines:
    def __init__(self, body: botocore.response.StreamingBody, initial_offset=0):
        self.body = body
        self.offset = initial_offset

    def iter_lines(self, chunk_size=1024):
        """Return an iterator to yield lines from the raw stream.
        This is achieved by reading chunk of bytes (of size chunk_size) at a
        time from the raw stream, and then yielding lines from there.
        """
        pending = b''
        for chunk in self.body.iter_chunks(chunk_size):
            lines = (pending + chunk).splitlines(True)
            for line in lines[:-1]:
                self.offset += len(line)
                yield line.decode('utf-8')
            pending = lines[-1]
        if pending:
            self.offset += len(pending)
            yield pending.decode('utf-8')
