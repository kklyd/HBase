package com.ruch.cloud.project;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("/api")
public class WebService {
 
public static final String TEAM_ID = "RuCh";
public static final String AWS_ACCOUNT_ID = "1234-5678-9101";

public static final String PORT = "60000";
//public static final String 

//heartbeat
//http://localhost:8080/q1
@GET
  @Path("/q1")
  @Produces(MediaType.TEXT_PLAIN)
  public String hearthbeat() {
    return TEAM_ID + ", " + AWS_ACCOUNT_ID + "\n" + "PUT DATE HERE";
  }

//search for tweets which are created at a specific second
//http://localhost:8080/q2?time=2013-10-02 20:28:08
@GET
  @Path("/q2")
  @Produces(MediaType.TEXT_PLAIN)
  public String findTweetsByTime(@QueryParam("time") String time) {
    return time;
  }

//find the total number of tweets sent by a range of userids
//http://localhost:8080/q3?userid_min=3435434&userid_max=2343434
@GET
  @Path("/q3")
  @Produces(MediaType.TEXT_PLAIN)
  public String findTweetsByUserIdRange(@QueryParam("userid_min") String userid_min, @QueryParam("userid_max") String userid_max) {
    return "userid_min: " + userid_min + "\n" + "userid_max: " + userid_max;
  }

//find the sets of userids who have retweeted any tweet posted by a given userid
//GET http://localhost:8080/q4?userid=1337100
@GET
  @Path("/q4")
  @Produces(MediaType.TEXT_PLAIN)
  public String findUsersWhoAreRetweetedUserId(@QueryParam("userid") String userid) {
    return "userid: " + userid;
  }
}
