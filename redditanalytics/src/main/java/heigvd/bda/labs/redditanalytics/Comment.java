package  heigvd.bda.labs.redditanalytics;

import org.json.JSONException;
import org.json.JSONObject;
import org.mortbay.util.ajax.JSON;

public class Comment {
	private JSONObject json;

	Comment(String json)
	{
		try {
			this.json = new JSONObject(json);
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
	
	Comment()
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

	public String getBody()
	{
		try {
			return (String) json.get("body");
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
		}
	}

	public String getLinkId()
	{
		try {
			return (String) json.get("link_id");
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
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
	
	public String getScore()
	{
		try {
			return json.get("score").toString();
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
		}
	}

	public String getParentId()
	{
		try {
			return (String) json.get("parent_id");
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
