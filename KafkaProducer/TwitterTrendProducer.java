package com.graphy.kafka;

import twitter4j.*;
import twitter4j.auth.AccessToken;

public class TwitterTrendProducer {


    public Trends read() throws Exception{
        AccessToken accessToken = new AccessToken("<Access Token Here>", "<Access Token Secret Here>");
        Twitter twitter = new TwitterFactory().getInstance();
        twitter.setOAuthConsumer("<Consumer Key (API Key)>", "<Consumer Secret (API Secret)>");
        twitter.setOAuthAccessToken(accessToken);
        ResponseList<Location> locations;
        locations = twitter.getAvailableTrends();
        System.out.println("Showing available trends");
        for (Location location : locations) {
            System.out.println(location.getName() + " (woeid:" + location.getWoeid() + ")");
        }
        Trends trends = twitter.getPlaceTrends(2295414);
        for (int i = 0; i < trends.getTrends().length; i++) {
            System.out.println(trends.getTrends()[i].getName());
        }
        return trends;
    }

    public static void main(String[] args) throws Exception{
        TwitterTrendProducer obj = new TwitterTrendProducer();
        Trends trend = obj.read();
    }
}
