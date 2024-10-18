import json
import boto3
import json
import re
import os
import datetime
import time
import logging
import dateutil
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
      
      
sqsClient = boto3.client('sqs')
sqsQurl = "https://sqs.us-east-1.amazonaws.com/445567075915/Q1"

def date_time_validator(date, time):
    return (dateutil.parser.parse(date).date() > datetime.date.today()) or (
            dateutil.parser.parse(date).date() == datetime.date.today() and dateutil.parser.parse(
        time).time() > datetime.datetime.now().time())
        
def date_checker(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False
    
def elicit_slot(session_state, intent_name, slots, slot_to_elicit, message_content):
    
    # Simplify the slot structure to send only non-null interpreted values
    simplified_slots = {
        slot: (slots[slot] if slots[slot] else None)
        for slot in slots
    }

    # Remove any slots that are None
    filtered_slots = {k: { "shape": "Scalar", "value": { "originalValue": v, "interpretedValue": v, "resolvedValues": [v]}} for k, v in simplified_slots.items() if v is not None}
    print(filtered_slots)
    response = {
        'sessionState': {
            'sessionAttributes': session_state.get('sessionAttributes', {}),
            'dialogAction': {
                'type': 'ElicitSlot',
                'slotToElicit': slot_to_elicit
            },
            'intent': {
                'name': intent_name,
                'slots': filtered_slots  # Only slots with values
            }
        },
        'messages': [
            message_content
        ]
    }
    print(response)
    return response
    
    
def ret_result(valid_flag, invalid_slot, message_):
    response = {
        'valid_flag':valid_flag,
        'invalid_slot':invalid_slot,
        'message' : {'contentType': 'PlainText', 'content': message_}
    }
    return response

      
def validate_values(loc, cuisine, people, date, time, email):
    locations = ['manhattan', 'nyc', 'ny']
    cuisine_types = ['indian', 'mexican', 'chinese', 'japanese', 'thai', 'continental']
    no_of_people = [str(i) for i in range(1,21)]
    no_ = ["one", "two", "three",
                     "four", "five", "six", "seven",
                     "eight", "nine", "ten", "eleven", "twelve",
                  "thirteen", "fourteen", "fifteen",
                  "sixteen", "seventeen", "eighteen",
                  "nineteen", "twenty"]
                  
    no_of_people.extend(no_)
    
    if not loc:
        return ret_result(False, 'Location', "Where are you looking to eat?")
    elif loc.lower() not in locations:
        return ret_result(False, 'Location', "Sorry, but we are currently serving only New York City area!")

    if not cuisine:
        return ret_result(False, 'Cuisine', "Great, What type of cuisine you're looking for?")
    elif cuisine.lower() not in cuisine_types:
        return ret_result(False, 'Cuisine', "Currently available cuisine options are - "+"["+", ".join(cuisine_types)+"]"+"\nPlease choose one of these!")

    if not people:
        return ret_result(False, 'NumberOfPeople', "Got it, how many people will be there?")
    elif str(people) not in no_of_people:
        return ret_result(False, 'NumberOfPeople', "We can accept booking for upto 20 people only, please enter the valid number")
        

    if not date:
        return ret_result(False, 'DiningDate', "Please tell me the date you are looking for the restaurant suggestions")
    elif datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
        return ret_result(False, 'DiningDate', "Oh snap, I can't book in the past as I don't have a timestone. You can look for the suggestions for any date from today onwards")
        
    if not time:
        return ret_result(False, 'DiningTime', "Ok, what time you are looking to dine out on " +str(datetime.datetime.strptime(date, '%Y-%m-%d').date())+" ?")
    elif not date_time_validator (date, time):
        return ret_result(False, 'DiningTime', "Unfortunately, I'm not a Dr. Strange, so can't book for time in the past. Please enter any time in the future!")
        
    if not email:
        return ret_result(False, 'Email', "Perfect!, just type in your E-mail address here so that I can send you the suggestions over there!")
    elif '@' not in email.lower():
        return ret_result(False, 'Email', "Please enter valid email address!")
            
    return ret_result(True, None, None)
    
    

def push_to_sqs(QueueURL, msg_body):
    """
    :param QueueName: String name of existing SQS queue
    :param msg_body: String message body
    :return: Dictionary containing information about the sent message. If
        error, returns None.
    """
    
    print("here in SQS func")
    
    sqs = boto3.client('sqs')

    queue_url = QueueURL
    try:
        # Send message to SQS queue
        response = sqs.send_message(
            QueueUrl=queue_url,
            DelaySeconds=0,
            MessageAttributes={
                'Location': {
                    'DataType': 'String',
                    'StringValue': "Manhattan"
                },
                'CuisineType': {
                    'DataType': 'String',
                    'StringValue': msg_body['Cuisine']
                },
                'NoOfPeople': {
                    'DataType': 'Number',
                    'StringValue': msg_body['NumberOfPeople']
                },
                'Date': {
                    'DataType': 'String',
                    'StringValue': msg_body['DiningDate']
                },
                'Time': {
                    'DataType': 'String',
                    'StringValue': msg_body['DiningTime']
                },
                'Email':{
                    'DataType':'String',
                    'StringValue' : msg_body['Email']
                }
            },
            MessageBody=(
                'Information about the diner'
            )
        )
    
    except ClientError as e:
        logging.error(e) 
        return None
    
    return response
    
    
def lambda_handler(event, context):
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    print("Event printed: ", event)
    if event["sessionState"]["intent"]["name"] == "GreetingIntent":
        
        answer = "Hi there, how can I help?"
    
        response = {
            "sessionState": {
                "dialogAction": {
                    "type": "Close"  # Close the conversation
                },
                "intent": {
                    "name": "GreetingIntent",  # Name of the intent
                    "state": "Fulfilled"  # Intent has been fulfilled
                }
            },
            "messages": [
                {
                    "contentType": "PlainText",
                    "content": answer
                }
            ]
        }
   
        return response
        
    if event["sessionState"]["intent"]["name"] == "ThankYouIntent":
        answer = "You are Welcome, See you soon!"
        
        response = {
            "sessionState": {
                "dialogAction": {
                    "type": "Close"  # Close the conversation
                },
                "intent": {
                    "name": "ThankYouIntent",  # Name of the intent
                    "state": "Fulfilled"  # Intent has been fulfilled
                }
            },
            "messages": [
                {
                    "contentType": "PlainText",
                    "content": answer
                }
            ]
        }
   
        return response
    
    
    if event["sessionState"]['intent']['name'] == 'DiningSuggestionsIntent':
       
        event_slots = event["sessionState"]['intent']['slots']
        source = event['invocationSource']
        
        if source == 'FulfillmentCodeHook':
            event_slots = event["sessionState"]['intent']['slots']
            slot_dict = {
                'Location': event_slots.get('Location', {}).get('value', {}).get('interpretedValue', ''),
                'Cuisine': event_slots.get('Cuisine', {}).get('value', {}).get('interpretedValue', ''),
                'NumberOfPeople': event_slots.get('NumberOfPeople', {}).get('value', {}).get('interpretedValue', ''),
                'DiningDate': event_slots.get('DiningDate', {}).get('value', {}).get('interpretedValue', ''),
                'DiningTime': event_slots.get('DiningTime', {}).get('value', {}).get('interpretedValue', ''),
                'Email': event_slots.get('Email', {}).get('value', {}).get('interpretedValue', '')
            }
            
        
            validated_result = validate_values(slot_dict["Location"], slot_dict["Cuisine"], slot_dict['NumberOfPeople'], slot_dict['DiningDate'], slot_dict['DiningTime'], slot_dict['Email'])
            
            if not validated_result['valid_flag']:
                slot_dict[validated_result['invalid_slot']] = None
                return elicit_slot(event['sessionState'], event["sessionState"]['intent']['name'], slot_dict, validated_result['invalid_slot'], validated_result['message'])
                
        broadcast = push_to_sqs(sqsQurl, slot_dict)
        
        if broadcast:
            response = {
                "sessionState": {
                    "dialogAction": {
                        "type": "Close"  # Close the conversation
                    },
                    "intent": {
                        "name": "DiningSuggestionsIntent",  # Name of the intent
                        "state": "Fulfilled"  # Intent has been fulfilled
                    }
                },
                "messages": [
                    {
                      "contentType":"PlainText",
                      "content": "That's great!! I have received your request of restaurant suggestions for {} cuisine. You will shortly receieve an E-mail at {} with the suggestions as per your preferences!".format(
                          slot_dict["Cuisine"], slot_dict['Email']),
                    }
                ]
            }
        else:
            response = {
                "sessionState": {
                    "dialogAction": {
                        "type": "Close"  # Close the conversation
                    },
                    "intent": {
                        "name": "DiningSuggestionsIntent",  # Name of the intent
                        "state": "Fulfilled"  # Intent has been fulfilled
                    }
                },
                "messages": [
                    {
                      "contentType":"PlainText",
                      "content": "Sorry, please come back later",
                    }
                ]
            }
        return response
