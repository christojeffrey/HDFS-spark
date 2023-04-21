FILENAME = "filenames.txt"
print("Reading filenames from " + FILENAME)

from pyspark.sql import SparkSession
import json
import datetime

# functions
def get_file_stating_filename(filename):
    starting_filenames = ['anaktester_go(error)', 'byu.id', 'gridoto_news', 'facebook_post', 'instagram_comment', 'instagram_media', 'instagram_post', 'instagram_status', 'myxl', 'telkomsel', 'twitter_status', 'youtube_comment', 'youtube_video']
    files_starting_filename = ''
    for starting_filename in starting_filenames:
        if starting_filename in filename:
            files_starting_filename = starting_filename
            break
    if files_starting_filename == '':
        raise Exception('files_starting_filename is empty')
    return files_starting_filename

def parse_to_number(df_row):
    filename = df_row[0]
    data = df_row[1]
    try:
        files_starting_filename = get_file_stating_filename(str(filename))
    except Exception as e:
        print("Error parsing filename: " + str(e))
        return None
    try:
        data = json.loads(data)
    except Exception as e:
        print("Error parsing json: " + str(e))
        # print first 100 line 
        print("data: " + str(data)[:100])
        return None
    # byu.id
    result = []
    if files_starting_filename == 'byu.id':
        SOCIAL_MEDIA = 'byu.id'
        typenames = data['GraphImages']
        for typename in typenames:
            DATE = typename['taken_at_timestamp']
            # PARSE 1644900422 TO 2021-03-01
            DATE = datetime.datetime.fromtimestamp(DATE).strftime('%Y-%m-%d')
            # print(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')
            result.append(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')
            for data in typename["comments"]["data"]:
                DATE = data['created_at']
                # PARSE 1644900422 TO 2021-03-01
                DATE = datetime.datetime.fromtimestamp(DATE).strftime('%Y-%m-%d')
                # print(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')
                result.append(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')

    # gridoto_news
    elif files_starting_filename == 'gridoto_news':
        SOCIAL_MEDIA = 'gridoto_news'
        typenames = data['GraphImages']
        for typename in typenames:
            DATE = typename['taken_at_timestamp']
            # PARSE 1644900422 TO 2021-03-01
            DATE = datetime.datetime.fromtimestamp(DATE).strftime('%Y-%m-%d')
            # print(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')
            result.append(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')
            for data in typename["comments"]["data"]:
                DATE = data['created_at']
                # PARSE 1644900422 TO 2021-03-01
                DATE = datetime.datetime.fromtimestamp(DATE).strftime('%Y-%m-%d')
                # print(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')
                result.append(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')
    # facebook_post
    elif files_starting_filename == 'facebook_post':
        SOCIAL_MEDIA = 'facebook_post'
        for datum in data:
            for comments in datum['comments']['data']:
                DATE = comments['created_time']
                # PARSE 2021-03-01T04:00:00+0000 TO 2021-03-01
                DATE = DATE.split('T')[0]
                # print(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')
                result.append(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')
    # instagram_comment
    elif files_starting_filename == 'instagram_comment':
        SOCIAL_MEDIA = 'instagram_comment'
        for datum in data:
            DATE = int(datum['created_time'])
            # PARSE 1644900422 TO 2021-03-01
            DATE = datetime.datetime.fromtimestamp(DATE).strftime('%Y-%m-%d')
            # print(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')
            result.append(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')
    # instagram_media
    elif files_starting_filename == 'instagram_media':
        SOCIAL_MEDIA = 'instagram_media'
        for datum in data:
            DATE = int(datum['created_time'])
            # PARSE 1644900422 TO 2021-03-01
            DATE = datetime.datetime.fromtimestamp(DATE).strftime('%Y-%m-%d')
            COUNT = str(1 + datum["comment"]["count"])
            # print(SOCIAL_MEDIA + '\t' + DATE + '\t' + COUNT)
            result.append(SOCIAL_MEDIA + '\t' + DATE + '\t' + COUNT)
    # instagram_post
    elif files_starting_filename == 'instagram_post':
        SOCIAL_MEDIA = 'instagram_post'
        for datum in data:
            DATE = int(datum['created_time'])
            # PARSE 1644900422 TO 2021-03-01
            DATE = datetime.datetime.fromtimestamp(DATE).strftime('%Y-%m-%d')
            COUNT =str(1 + datum["comment"]["count"])
            # print(SOCIAL_MEDIA + '\t' + DATE + '\t' + COUNT)
            result.append(SOCIAL_MEDIA + '\t' + DATE + '\t' + COUNT)

    # instagram_status
    elif files_starting_filename == 'instagram_status':
        SOCIAL_MEDIA = 'instagram_status'
        for datum in data:
            DATE = int(datum['created_time'])
            # PARSE 1644900422 TO 2021-03-01
            DATE = datetime.datetime.fromtimestamp(DATE).strftime('%Y-%m-%d')
            COUNT = str(1 + datum["comment"]["count"])
            # print(SOCIAL_MEDIA + '\t' + DATE + '\t' + COUNT)
            result.append(SOCIAL_MEDIA + '\t' + DATE + '\t' + COUNT)
    # myxl
    elif files_starting_filename == 'myxl':
        SOCIAL_MEDIA = 'myxl'
        data = data['GraphImages']
        for datum in data:
            comments = datum['comments']['data']
            for comment in comments:
                DATE = int(comment['created_at'])
                # PARSE 1644900422 TO 2021-03-01
                DATE = datetime.datetime.fromtimestamp(DATE).strftime('%Y-%m-%d')
                # print(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')
                result.append(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')
    # telkomsel
    elif files_starting_filename == 'telkomsel':
        SOCIAL_MEDIA = 'telkomsel'
        data = data['GraphImages']
        for datum in data:
            comments = datum['comments']['data']
            for comment in comments:
                DATE = int(comment['created_at'])
                # PARSE 1644900422 TO 2021-03-01
                DATE = datetime.datetime.fromtimestamp(DATE).strftime('%Y-%m-%d')
                # print(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')
                result.append(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')
    # twitter_status
    elif files_starting_filename == 'twitter_status':
        SOCIAL_MEDIA = 'twitter_status'
        for datum in data:
            DATE = datum['created_at']
            # Fri Jan 01 05:03:05 +0000 2021 parse to 2021-01-01
            DATE = DATE.split(' ')[5] + '-' + DATE.split(' ')[1] + '-' + DATE.split(' ')[2]
            DATE = datetime.datetime.strptime(DATE, '%Y-%b-%d').strftime('%Y-%m-%d')

            # print(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')
            result.append(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')



        pass
    # youtube_comment
    elif files_starting_filename == 'youtube_comment':
        SOCIAL_MEDIA = 'youtube_comment'
        for datum in data:
            # if doesn't have publishedAt, then skip
            if 'publishedAt' not in datum['snippet']:
                continue
            DATE = datum['snippet']['publishedAt']
            # PARSE 2021-03-01T04:00:00.000Z TO 2021-03-01
            DATE = DATE.split('T')[0]
            # print(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')
            result.append(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')
    # youtube_video
    elif files_starting_filename == 'youtube_video':
        SOCIAL_MEDIA = 'youtube_video'
        for datum in data:
            # if doesn't have publishedAt, then skip
            if 'publishedAt' not in datum['snippet']:
                continue
            DATE = datum['snippet']['publishedAt']
            # PARSE 2021-03-01T04:00:00.000Z TO 2021-03-01
            DATE = DATE.split('T')[0]
            # print(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')
            result.append(SOCIAL_MEDIA + '\t' + DATE + '\t' + '1')
    
    return result


# algorithm
spark = SparkSession.builder.appName("MyApp").getOrCreate()

#  Read in file names
filenames = spark.read.text("hdfs://namenode:9000/data/" + FILENAME)

# 0. setup content list
content = []

for filename in filenames.rdd.collect():
    example = spark.read.text("hdfs://namenode:9000/data/raw_json/" + filename['value'])

    data = example.rdd.map(lambda x: x['value']).collect(
    )
    data = ''.join(data)
    content.append((filename['value'], data))

# Convert content list to PySpark DataFrame
df = spark.createDataFrame(content, ['filename', 'content'])

# 1. map
mapped = df.rdd.map(parse_to_number).collect()


flatMapped = []
for sublist in mapped:
    # check if sublist is list
    if type(sublist) == list:
        for item in sublist:
            flatMapped.append(item)
    
print("DONE MAP")

# 2. reduce

counter = {}
for line in flatMapped:
    socialMedia, time, count = line.split("\t")

    if socialMedia not in counter:
        counter[socialMedia] = {}
    if time not in counter[socialMedia]:
        counter[socialMedia][time] = 0

    counter[socialMedia][time] += int(count)

# print
# for socialMedia in counter:
#     for time in counter[socialMedia]:
#         print(socialMedia + "\t" + time + "\t" + str(counter[socialMedia][time]))


# output to csv
csv = open('output.csv', 'w')
csvString = 'socialMedia,time,count\n'
for socialMedia in counter:
    for time in counter[socialMedia]:
        csvString += socialMedia + "," + time + "," + str(counter[socialMedia][time]) + "\n"

csv.write(csvString)
