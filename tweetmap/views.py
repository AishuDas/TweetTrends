from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from elasticsearch import Elasticsearch
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from tweepy import Stream
import json
import urllib3
from elasticsearch import Elasticsearch, RequestsHttpConnection
import time
import geocoder
from django.views.decorators.csrf import csrf_protect, csrf_exempt
from django.views.decorators.cache import cache_page
from textwrap import TextWrapper
import requests
from django.template import*
from django.template import Context
from django.template.loader import get_template
import boto3
import time
import traceback
from requests_aws4auth import AWS4Auth
from textwrap import TextWrapper
import cgi
import sys



def index(request):
    return render(request, 'tweetmap/header.html')

# authentication details for AWS Elasticsearch
host = "http://search-tweetmap-3irxvi2quadmyikuw2vh6227rq.us-east-1.es.amazonaws.com/test-index/tweet/_search?size=10000&q="


def gettweet(request):
    string = request.GET.get("abc")

    def search(link, key):
        final_link = link + key
        response = requests.get(final_link)
        results = json.loads(response.text)
        return results

    r = search(host, string)

    coordinate1 = []
    data = r['hits']['hits']

    for i in data:
        coordinate1.append(i['_source']['coordinates']['coordinates'])
        #sentiment = i['_source']['sentiment']

    list = Context({'coordinatelist': coordinate1})

    return render(request, "tweetmap/index.html", {"coordinatelist": coordinate1})

def confirmSubscription(token,topic_arn):
    sns = boto3.client('sns',aws_access_key_id= 'AKIAJTBAXPOSWN733BIA',
                        aws_secret_access_key = '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',
                        region_name = 'us-east-1')
    try:
        response = sns.confirm_subscription(
            TopicArn=topic_arn,
            Token=token
        )
        print response
        # return jsonify({"status": "ok"})
    except:
        pass

@csrf_exempt
def sns_handler(request):
    if request.method=="GET":
        return render(request, 'tweetmap/header.html')
    else:
        headers = json.loads(request.body.decode("utf-8"))
        print headers
        hdr = headers['Type']
        if hdr =="SubscriptionConfirmation":
            url = headers['SubscribeURL']
            print("Subscription Confirmation - Visiting URL : " + url)
            http = urllib3.PoolManager()
            r = http.request('GET', url)
            print(r.status)
            token = headers["Token"]
            topic_arn = headers["TopicArn"]
            confirmSubscription(token, topic_arn)
            print("Subscribed to SNS")
        elif hdr =="Notification":
            print ("Received a new message: "+str(headers["Message"]))
            message = json.loads(json.loads(headers["Message"]).get('default'))
            print ("Message :"+str(message))
            id = message.get('id')
            tweet = message.get('tweet')
            lat = message.get("lat")
            lng = message.get("lng")
            sentiment = message.get("sentiment")
            es = get_es_connection()
            es.index(index='tweet-index', id=id, doc_type="tweet",body={"tweet": tweet, "location": {"lat": lat, "lng": lng}, "sentiment": sentiment})
    return render(request, 'tweetmap/header.html')

def get_es_connection():

    host = 'search-tweetmap-3irxvi2quadmyikuw2vh6227rq.us-east-1.es.amazonaws.com'
    awsauthentication = AWS4Auth('AKIAJTBAXPOSWN733BIA', 'NZAQX0vBV541SNWsewaRSUKtOa1yTps1DToo5VpJ', 'us-east-1', 'es')

    es = Elasticsearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauthentication,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
    return es