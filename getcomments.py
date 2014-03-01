import urllib2
import json
import datetime
import time
import pytz
import pandas as pd
import os
from pandas import DataFrame

filename = 'hacker_news_comments.csv'

def update_csv(f, df):
	df["comment_text"] = df["comment_text"].map(lambda x: x.translate(dict.fromkeys([0x201c, 0x201d, 0x2011, 0x2013, 0x2014, 0x2018, 0x2019, 0x2026, 0x2032])).encode('utf-8').replace(',',''))
	df["created_at"] = df["created_at_i"].map(lambda x: datetime.datetime.fromtimestamp(int(x), tz=pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S'))

	ordered_df = df[["comment_text","points","author","created_at","objectID"]]

	ordered_df.to_csv(f,encoding='utf-8', index=False)


if os.path.isfile(filename):
    os.remove(filename)

ts = str(int(time.time()))
df = DataFrame()
hitsPerPage = 1000
requested_keys = ["author", "comment_text", "created_at_i", "objectID", "points"]

i = 0

while True:
	with open(filename, 'a') as f:
		try:
			url = 'https://hn.algolia.com/api/v1/search_by_date?tags=comment&hitsPerPage=%s&numericFilters=created_at_i<%s' % (hitsPerPage, ts)
			req = urllib2.Request(url)
			response = urllib2.urlopen(req)
			data = json.loads(response.read())
			last = data["nbHits"] < hitsPerPage
			data = DataFrame(data["hits"])[requested_keys]
			df = df.append(data,ignore_index=True)
			ts = data.created_at_i.min()
			print i
			if (last):
				update_csv(f, df)
				break
			if (i % 2 == 0): # We write occasionally
				update_csv(f, df)
				df = DataFrame()				

			time.sleep(3.6)
			i += 1

		except Exception, e:
			print e