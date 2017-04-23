import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from requests_aws4auth import AWS4Auth
from textwrap import TextWrapper
import boto3
import time
import traceback
import cgi
import sys
from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
	help = 'passes tweets to SQS'

	def handle(self, *args, **options):
		# twitter keys for access
		ckey="oQ2AyNZY2pOj7eZhmeMnPWl0t"
		csecret="BXoJkxMvlG5l73NTrcojclMZqljugEQJFnsQO2pMN69HIW0vCK"

		atoken="416868353-RHo8NNvT1Jyy71gQQfu5tH95h8mCvx1LJUkv88mb"
		atoken_secret="dinDhf13RlCHHuQZA983LOy102VOWm3bXjhzqkBHnFFF4"

		# AWS keys
		AWSaccess = "AKIAJTBAXPOSWN733BIA"
		AWSSecret = "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"

		count = 0

		sqs = boto3.resource('sqs',
							 aws_access_key_id= AWSaccess,
							 aws_secret_access_key = AWSSecret,
		)

		try:
			queue = sqs.get_queue_by_name(QueueName='tweets')
		except:
			queue = sqs.create_queue(QueueName = 'tweets')
		print(queue.url)


		class SListener(StreamListener):
			def on_data(self, raw_data):
				global count
				try:
					tweets_json = json.loads(raw_data)
					try:
						if ('coordinates' in tweets_json):
							if tweets_json["coordinates"] is not None:
								lon = tweets_json["coordinates"]['coordinates'][0]
								lat = tweets_json["coordinates"]['coordinates'][1]
								text = tweets_json["text"]
								# text = text.encode(encoding='utf-8')
								tweet_json = {
									"tweet": text,
									"lat": lat,
									"lng": lon,
									"id": tweets_json['id']
								}
								json_dump = json.dumps(tweet_json)
								if (count < 200):
									res = queue.send_message(MessageBody=str(json_dump))
									count = count + 1
									print(res)
								else:
									print(count)
					except:
						print('error')
						return True
					return True
				except:
					print('error')
					traceback.print_exc()
					time.sleep(1)

			def on_error(self, status_code):
				print(status_code)


		m = SListener()
		auth = OAuthHandler(ckey, csecret)
		auth.set_access_token(atoken, atoken_secret)
		stream = Stream(auth, m)

		try:
			stream.filter(track=['trump','usa','snapchat','football','india','modi','logan','facebook','elections','india','news','messi','money','food','car'])
		except Exception, e:
			pass
