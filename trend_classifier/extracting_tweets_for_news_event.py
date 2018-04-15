import random

def get_trend_data():
    f = open('TT-annotations.csv', 'r',encoding='utf-8')
    trend_data = []
    c=0
    for l in f.readlines():
        c+=1
        l = l.strip()
        if l.count(";") >3:
            print(c)
        trend_details = l.split(';')
        trend_id = trend_details[0]
        trend_name=trend_details[2]
        trend_label = trend_details[3]
        if trend_label=='news' or trend_label=='ongoing-event':
            trend_data.append([trend_id, trend_name, trend_label])

    f.close()

    return trend_data

def print_trend_info(partial_data):
    f_trend_out = open('trend_info.txt', 'w',encoding='utf-8')
    for [trend_id,trend_name, trend_label] in partial_data:
        f_trend_out.write('%s;%s;%s\n' % (trend_id, trend_name, trend_label))

    f_trend_out.close()

def extract_tweetIDs_for_trends(N=10):
    f_trend=open('trend_info.txt', 'r',encoding='utf-8')
    f_tweet_out=open('tweet_info.txt', 'w',encoding='utf-8')
    for l in f_trend.readlines():
        l = l.strip()
        trend_details = l.split(';')
        trend_id = trend_details[0]
        trend_label = trend_details[2]
        with open('tweets'+'//'+trend_id, 'r',encoding='utf-8') as f_tweet:
            lines = f_tweet.readlines()
        random.shuffle(lines)
        for l_tweet in lines[:N]:
            tweet_ID=l_tweet.strip().split('\t')[0]
            f_tweet_out.write('%s %s\n' % (tweet_ID,trend_label))

    f_tweet_out.close()


if __name__== "__main__":
    partial_data=get_trend_data()
    print(len(partial_data))

    print_trend_info(partial_data)
    extract_tweetIDs_for_trends()