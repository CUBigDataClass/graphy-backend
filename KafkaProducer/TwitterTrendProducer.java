package com.graphy.kafka;

import twitter4j.*;
import twitter4j.auth.AccessToken;

public class TwitterTrendProducer extends Thread{


    public Trends read() throws Exception {
        AccessToken accessToken = new AccessToken("3161321025-FajWBXoXT4PAZGNyta38w4krEsK8fp2d5opwdN4", "p2BPY6BzUKyQJRmIMDUvAwPXjBCksRTBI12sYi5xdkvrC");
        Twitter twitter = new TwitterFactory().getInstance();
        twitter.setOAuthConsumer("5VS16zsltlmES0keEq860Xkut", "m6MFR7FTxn2ON3NrMFALD48DFy8ngVxO1KXfE8g1qhaergxpp2");
        twitter.setOAuthAccessToken(accessToken);
        ResponseList<Location> locations;
        locations = twitter.getAvailableTrends();
        System.out.println("Showing available trends");
        int countWoeid = 0;
        int array[] = new int[1000];
        for (Location location : locations) {
            System.out.println(location.getName() + " (woeid:" + location.getWoeid() + ")");
            array[countWoeid] = location.getWoeid();
            countWoeid++;
        }
        int countTrends = 0;
        Trends trendsRet = twitter.getPlaceTrends(2295414);
        for (int j = 0; j < 468; j++){
            Trends trends = twitter.getPlaceTrends(array[j]);
            for (int i = 0; i < trends.getTrends().length; i++) {
                System.out.println(trends.getTrends()[i].getName());
                System.out.println("Current location number out of 467 = "+j);
                System.out.println("Total number of trends extracted till now = "+countTrends);
                countTrends++;
                if (countTrends%1000==0)
                    Thread.sleep(120000);// Sleep for 2 minutes after extracting every 1000 trends
        }
    }
        System.out.println("Total number of locations = "+countWoeid);
        System.out.println("Total number of trends till now = "+countTrends);
        return trendsRet;
    }

    public static void main(String[] args) throws Exception{
        TwitterTrendProducer obj = new TwitterTrendProducer();
        Trends trend = obj.read();
    }
}
