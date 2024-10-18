import json
import boto3
from botocore.exceptions import ClientError
import uuid
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')

lex_client = boto3.client('lexv2-runtime') 

def db_name_check(name):
    table = dynamodb.Table('user_preferences')
    response = table.query(
        KeyConditionExpression=Key('user_name').eq(name)
    )
    print("response items in db_name_check_func", response)
    return response['Items'][0]

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

def lambda_handler(event, context):
    
  try:
    # Extract the user's message from the API request
    print("Event: ",event)
    user_message = event['messages'][0]['unstructured']['text']  # Assuming the API request contains a "message" field
    
    try:
      last_reco = db_name_check("user1")
    except Exception as e:
      last_reco =  None
        
    if (last_reco) and (str(user_message).lower() == "hi can i get 5 more suggestions?"):
      restaurant_list = ""
      for idx, restaurant in enumerate(last_reco["last_reco"], 1):  # Enumerate starts numbering from 1
          restaurant_list += f"{idx}. {restaurant['name']} - {restaurant['display_address']}<br>"
      
      # Final formatted response
      text_response = f"Hey, as per our last interaction, my suggestions for {last_reco['cuisine']} cuisine are: <br>{restaurant_list}"
      text_response += f"<br> I have also sent an email with the suggestions."
      response = {
      "messages": [
        {
          "type": "unstructured",
          "unstructured": {
            "id": 1,
            "text": text_response,
            # 'text' : "abc"
            "timestamp": "03-03-2022"
            }
        }
        ]
      }
      
      print(last_reco["last_reco"],last_reco["email"], last_reco["cuisine"])
      
      send_email(last_reco["last_reco"],last_reco["email"], last_reco["cuisine"])
      return response
    
    else:  
      # Define Lex parameters
      bot_id = '********'          # Replace with your Lex bot's ID
      bot_alias_id = '*******'     # Replace with your Lex bot alias ID
      locale_id = 'en_US'          # Set to your bot's locale
      session_id = 'user1'         # Use a unique session ID per user or session
      
      # Call Lex to get a response
      response = lex_client.recognize_text(
          botId=bot_id,
          botAliasId=bot_alias_id,
          localeId=locale_id,
          sessionId=session_id,
          text=user_message
      )
      
      print("Response: ",response)
      
      # Extract the message from Lex's response
      lex_message = response['messages'][0]['content']
      
      
      response = {
        "messages": [
          {
            "type": "unstructured",
            "unstructured": {
              "id": 1,
              "text": lex_message,
              # 'text' : "abc"
              "timestamp": "03-03-2022"
            }
          }
        ]
      }
      return response
    
  except ClientError as e:
    print(f"Error: {str(e)}")
    
    return {
        'statusCode': 500,
        'body': json.dumps({
            'error': 'Something went wrong!'
        }),
        'headers': {
            'Content-Type': 'application/json'
        }
    } 

  return response

