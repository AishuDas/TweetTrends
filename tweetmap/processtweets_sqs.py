import traceback
# TODO:  Find sentiment of tweets and push in SNS
import boto3
import time
import json
from monkeylearn import MonkeyLearn


ml = MonkeyLearn('11676ef20ce5b1a6ec1a0f4fa22d2c35a5ed641f')
count = 0;
sqs = boto3.resource('sqs')
sns = boto3.client('sns')

snsarn = 'arn:aws:sns:us-east-1:879072304802:Analysed_tweets'

queue = sqs.get_queue_by_name(QueueName='tweets')
print(queue.url)

def find_sentiment(tweet, count=0):
    # TODO call monkeylearn api to find sentiment here
    if (count < 500):
        try:
            module_id = 'cl_qkjxv9Ly'
            text_list = [tweet]
            print (text_list)
            res = ml.classifiers.classify(module_id, text_list, sandbox=True)
            print("Sentiment: ", res.result[0][0]['label'])
            sentiment=res.result[0][0]['label']
            if sentiment == 'positive':
                return "1"
            elif sentiment == 'negative':
                return "-1"
            return "0"
        except:
            return "0"
    return "0"

while True:
    m = queue.receive_messages()
    if(len(m)>0):
        raw_data = m[0].body
    else:
        continue
    raw_data = json.loads(raw_data)
    print (raw_data)
    id = raw_data['id']
    tweet = raw_data['tweet']
    lat = raw_data['lat']
    lng = raw_data['lng']
    #print (tweet)
    sentiment = find_sentiment(tweet,count)
    count = count+1
    #message = "tweet:-"+raw_data['tweet']+"||"+"lat:-"+str(raw_data['lat'])+"||"+"lng:-"+str(raw_data['lng'])+"||"+"id:-"+str(raw_data['id'])+"||"+"sentiment:-"+str(sentiment)
    sns_message = {"id":id, "tweet":tweet, "lat":lat, "lng": lng, "sentiment":sentiment}
    print (sns_message)
    #try:
    response = sns.publish(
    TopicArn=snsarn,
    Message= json.dumps({'default':json.dumps(sns_message)}),
    )
    print("done")
    #response = m[0].delete()
    print(response)
    #except:
    print('error')
    #pass