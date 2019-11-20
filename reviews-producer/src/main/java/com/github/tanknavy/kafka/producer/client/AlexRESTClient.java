package com.github.tanknavy.kafka.producer.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.http.HttpException;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;

import com.github.tanknavy.avro.Course;
import com.github.tanknavy.avro.Review;
import com.github.tanknavy.avro.User;
import com.github.tanknavy.kafka.producer.model.ReviewApiResponse;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;


/**
 * https://www.udemy.com/api-2.0/courses/1075642/reviews
 * @author admin
 *
 */

public class AlexRESTClient {
	private Integer count;
	private String courseId;
	private Integer nextPage;
	private final Integer pageSize; // 如果修改后面就无法正确定位了
	
	public AlexRESTClient(String courseId, Integer pageSize) {
		super();
		this.courseId = courseId;
		this.pageSize = pageSize;
	}
	
	private void init() throws HttpException{
		count = reviewApi(1,1).getCount(); // 一共多少页
		nextPage = count / pageSize + 1; //获取最后一页
	}
	
	public List<Review> getNextReviews() throws HttpException {
		// TODO Auto-generated method stub
		if (nextPage == null) init();
		if (nextPage >=1) {
			List<Review> result = reviewApi(pageSize, nextPage).getReviewList();
			nextPage -= 1; // 在外面可以迭代调用
			
			return result;
		}
		return Collections.emptyList(); //如果为空
	}
	
	


	public ReviewApiResponse reviewApi(int pageSzie, int page) throws HttpException {
		// TODO Auto-generated method stub
		String url = "https://www.udemy.com/api-2.0/courses/" + courseId + "/reviews";
		HttpResponse<JsonNode> jsonResponse = null;
		try{
			jsonResponse = Unirest.get(url)
					.queryString("page", page)
					.queryString("page_size", pageSzie)
					.queryString("fields[course_review]", "title,content,rating,created,modified,user_modified,user,course")// 注意拼写
					.asJson();
		}catch(UnirestException e){
			throw new HttpException(e.getMessage());
		}
		
		if(jsonResponse.getStatus() == 200){
			JSONObject body = jsonResponse.getBody().getObject();
			Integer count = body.getInt("count"); // 前多少个
			String next = body.optString("next");	
			String previos = body.optString("previous");
			// title是嵌套的格式
			List<Review> reviews = this.convertResults(body.getJSONArray("results")); // 转换results结果集
			ReviewApiResponse reviewApiResponse = new ReviewApiResponse(count, next, previos, reviews);
			
			return reviewApiResponse;
		}
		
		throw new HttpException("website API Unvailable");
		//return null ;
		
	}
    
	// 响应的JsonArray转换为List
	private List<Review> convertResults(JSONArray resultsJsonArray) {
		// TODO Auto-generated method stub
		List<Review> results = new ArrayList<Review>();
		for (int i=0; i< resultsJsonArray.length();i++){
			JSONObject reviewJson = resultsJsonArray.getJSONObject(i);
			Review review = jsonToReview(reviewJson);
			results.add(review);
		}
		results.sort(Comparator.comparing(Review::getCreated));;
		return results;
	}

	public Review jsonToReview(JSONObject reviewJson) {
		// TODO Auto-generated method stub
		Review.Builder reviewBuilder = Review.newBuilder(); // avro序列化
		reviewBuilder.setContent(reviewJson.getString("content"));
		reviewBuilder.setId(reviewJson.getLong("id"));
		reviewBuilder.setRating(reviewJson.getBigDecimal("rating").toPlainString());
		
		//"user":{"_class":"user","title":"Ganiyu Babatunde","name":"Ganiyu","display_name":"Ganiyu Babatunde"}
		reviewBuilder.setTitle(reviewJson.getString("content")); // titile嵌套在user里面了
		//reviewBuilder.setTitle(jsonToUser(reviewJson.getJSONObject("user")).getTitle()); // titile嵌套在user里面了
		//reviewBuilder.setTitle(jsonToUser(reviewJson.getString("user"))); // titile嵌套在user里面了
		
		reviewBuilder.setCreated(DateTime.parse(reviewJson.getString("created")));
		reviewBuilder.setModified(DateTime.parse(reviewJson.getString("modified")));
		reviewBuilder.setUser(jsonToUser(reviewJson.getJSONObject("user")));
		reviewBuilder.setCourse(jsonToCourse(reviewJson.getJSONObject("course")));
		
		return reviewBuilder.build();
	}

	public User jsonToUser(JSONObject userJson) {
		// TODO Auto-generated method stub
		User.Builder userBuilder = User.newBuilder();
		userBuilder.setTitle(userJson.getString("title"));
		userBuilder.setName(userJson.getString("name"));
		userBuilder.setDisplayName(userJson.getString("display_name"));
		return userBuilder.build();
	}

	public Course jsonToCourse(JSONObject courseJson) {
		// TODO Auto-generated method stub
		Course.Builder courseBuilder = Course.newBuilder();
		courseBuilder.setId(courseJson.getLong("id"));
		courseBuilder.setTitle(courseJson.getString("title"));
		courseBuilder.setUrl(courseJson.getString("url"));
		return courseBuilder.build();
	}
	
	public void close(){
		try{
			Unirest.shutdown(); // 关掉http访问
		} catch(IOException e){
			e.printStackTrace();
		}
	}




	
	
}
