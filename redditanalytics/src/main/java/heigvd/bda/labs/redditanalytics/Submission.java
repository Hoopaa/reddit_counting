package  heigvd.bda.labs.redditanalytics;

import org.json.JSONException;
import org.json.JSONObject;

public class Submission {
	private JSONObject json;

	Submission(String json)
	{
		try {
			this.json = new JSONObject(json);
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	Submission()
	{
		try {
			this.json = new JSONObject("{}");
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	public void setJson(String json)
	{
		try {
			this.json = new JSONObject(json);
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	public String getId()
	{
		try {
			return (String) json.get("id");
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
		}
	}

	public String getTitle()
	{
		try {
			return (String) json.get("title");
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
		}
	}

	public String getSubredditId()
	{
		try {
			return (String) json.get("subreddit_id");
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
		}
	}

	public String getScore()
	{
		try {
			return json.get("score").toString();
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public String getNumComments()
	{
		try {
			return json.get("num_comments").toString();
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public String getBody()
	{
		try {
			return (String) json.get("selftext");
		} catch (JSONException e) {
			return "";
		}
	}
	
	
	public String getComment(){
		try {
			return json.get("comment").toString();
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public String getAll()
	{
		return json.toString();
	}

}
