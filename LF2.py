import json
import boto3
import decimal
import random
from botocore.exceptions import ClientError
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
#import constants 

#OpenSearch / Elastic Search query part
REGION = 'us-east-1'
HOST = 'search-restaurants-domain-bfaqu2hsgnnvgmsspl77vr237u.aos.us-east-1.on.aws'
INDEX = 'id'

def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth('*************',
                    '*******************',
                    region,
                    service
                    )

def elasticquery(term):
    q = {'size': 100, 'query': {'multi_match': {'query': term}}}

    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
        http_auth=get_awsauth(REGION, 'es'),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection)

    res = client.search(index='restaurants', body=q)

    hits = res['hits']['hits']
    results = []
    for hit in hits:
        # results.append(hit['_source']['cusine'])
        results.append(hit['_id'])
        
    return results

def add_user_prefs(cuisinetype, recos, email):
    #aws_access_key_id = '**************'
    #aws_secret_access_key = '*****************'

    dynamodb = boto3.resource('dynamodb')
    try:
        table = dynamodb.Table('user_preferences')
        new_data = {"user_name":"user1", "cuisine":cuisinetype, "last_reco":recos, "email":email}
        table.put_item(Item=new_data)
        return "Success"
    except Exception as e:
        print("exceptions in add_db", e)
        return "Failed"
        
        
def send_email(restaurants,email, cuisine):
    try:
        client = boto3.client('ses')
        mailBody = "Greetings, \n\n Here are 5 "+ f'{cuisine}'+" restaurants matching your preferences in Manhattan\n\n"
        for idx, restaurant in enumerate(restaurants, 1):
            mailBody += f'{idx}. {restaurant["name"]} - {restaurant["display_address"]}\n'

        mailBody += '\n Have a wonderful day! - Your Dining Bot\n'
        response = client.send_email(
            Source='vb2409@nyu.edu',
            Destination={
                'ToAddresses': [
                   email,
                ]},
                Message={
                'Subject' : {
                    'Data' : "Dining Concierge Restaurant Recommendations"
                },
                'Body' :{
                    'Text' :{
                       'Data' : str(mailBody)
                    }
                }
            }
        )
        print('email sent')
    except KeyError:
        logger.debug("Error sending ")



def replace_decimals(obj):
    if isinstance(obj, list):
        for i in range(0,len(obj)):
            obj[i] = replace_decimals(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k in obj.keys():
            obj[k] = replace_decimals(obj[k])
        return obj
    elif isinstance(obj, decimal.Decimal):
        return str(obj)
    else:
        return obj

def get_dynamo_data(dynno, table, key):
    print('Inside get_dynamo_data',dynno, table, key)
    response = table.get_item(Key={'BusinessID':key}, TableName='yelp-restaurants')
    # print(response)
    response = replace_decimals(response)
    # print('abcd')
    print('Response',response)
    
    name = response['Item']['Name'] if response['Item']['Name'] else None
    rating = response['Item']['Rating'] if response['Item']['Rating'] else None
    display_address = response['Item']['Address'] if  response['Item']['Address'] else None
    # review_count = response['Item']['review_count'] if  response['Item']['review_count'] else None
    # coordinates = response['Item']['coordinates']  if response['Item']['coordinates']  else None
    
    return {"name":name,
            "rating": rating,
            # "price": price,
            "display_address":display_address,
            # "review_count": review_count,
            # "coordinates":coordinates
            } 


def lambda_handler(event=None, context=None):
    # TODO implement

    # Fetch Query from SQS
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/445567075915/Q1'
    
    
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'time', 'cuisine', 'location', 'num_people', 'email'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )
    messages = response['Messages'] if 'Messages' in response.keys() else []
    print('Messages',messages)
            
    while messages:
        message = messages.pop()
        msg_attributes=message['MessageAttributes']
        query = {"query": {"match": {"Cuisine": msg_attributes["CuisineType"]["StringValue"]}}}
        email = msg_attributes["Email"]["StringValue"]
        chat_cusine =  msg_attributes["CuisineType"]["StringValue"]
        print(email, chat_cusine)

        # Fetch the IDS from elasticsearch
        ids = elasticquery(chat_cusine)
        print('Ids',ids)
        restaurant_id_indices = random.sample(ids,5)

        # init dynamodb details
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('yelp-restaurants')
        
        restaurants_set = []

        # iterate through indices and get details 
        print('RESTAURANT IDS',restaurant_id_indices)
        for id in restaurant_id_indices:
            suggested_restaurant = get_dynamo_data(dynamodb, table, id)
            restaurants_set.append(suggested_restaurant)
            
        print('Restaurant details',restaurants_set)
        
        send_email(restaurants_set,email,chat_cusine)
    
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=message['ReceiptHandle']
        )
        
        ### Store the User Preferences in DynamoDB
        
        restaurant_id_indices = random.sample(ids,5)
        
        # init dynamodb details
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('yelp-restaurants')
        
        restaurants_set = []

        # iterate through indices and get details 
        print('RESTAURANT IDS',restaurant_id_indices)
        for id in restaurant_id_indices:
            suggested_restaurant = get_dynamo_data(dynamodb, table, id)
            restaurants_set.append(suggested_restaurant)
    
        new_db_add = add_user_prefs(chat_cusine, restaurants_set, email)
        print("New db add status", new_db_add)
        print("Restaurants added to DB", restaurants_set)
    
    return {
        'statusCode': 200,
        'body': ''
    }
