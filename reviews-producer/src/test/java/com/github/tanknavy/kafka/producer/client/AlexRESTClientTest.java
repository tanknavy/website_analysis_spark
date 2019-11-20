package com.github.tanknavy.kafka.producer.client;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.http.HttpException;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import com.github.tanknavy.avro.Course;
import com.github.tanknavy.avro.Review;
import com.github.tanknavy.avro.User;
import com.github.tanknavy.kafka.producer.model.ReviewApiResponse;
import com.mashape.unirest.http.JsonNode;

public class AlexRESTClientTest {
	
	private AlexRESTClient alexRESTClient;
	
	@Before
	public void setUp() throws Exception {
		alexRESTClient = new AlexRESTClient("1075642", 30);
	}

	@Test
	public void apiCallIsSucessful() throws HttpException {
		ReviewApiResponse response = alexRESTClient.reviewApi(3, 2);
		assertTrue(response.getCount() >0);
		assertTrue(response.getNext() != null);
        assertTrue(response.getPrevios() != null);
        List<Review> reviews = response.getReviewList();
        assertTrue(reviews.size() == 3);
        assertTrue(reviews.get(0).getCreated().isBefore(reviews.get(1).getCreated()));
		//fail("Not yet implemented");
	}
	
	
	@Test
    public void canParseReviewJson(){
        JsonNode json = new JsonNode("{\n" +
                "\"_class\": \"course_review\",\n" +
                "\"id\": 6225498,\n" +
                "\"title\": \"\",\n" +
                "\"content\": \"\",\n" +
                "\"rating\": 4.0,\n" +
                "\"created\": \"2017-03-30T20:13:54Z\",\n" +
                "\"modified\": \"2017-03-31T21:23:04Z\",\n" +
                "\"user\": {\n" +
                "\"_class\": \"user\",\n" +
                "\"title\": \"Renato Dias Santana\",\n" +
                "\"name\": \"Renato Dias\",\n" +
                "\"display_name\": \"Renato Dias Santana\"\n" +
                "},\n" +
                "\"course\": {\n" +
                "\"_class\": \"course\",\n" +
                "\"id\": 1075642,\n" +
                "\"title\": \"Apache Kafka Series - Learn Apache Kafka for Beginners\",\n" +
                "\"url\": \"/apache-kafka-series-kafka-from-beginner-to-intermediate/\"\n" +
                "}\n" +
                "}");

        Review review = alexRESTClient.jsonToReview(json.getObject());
        assertTrue(review.getId() == 6225498L);
        assertEquals(review.getTitle(), "");
        assertEquals(review.getContent(), "");
        assertEquals(review.getRating(), "4.0");
        assertEquals(review.getCreated(), DateTime.parse("2017-03-30T20:13:54Z"));
        assertEquals(review.getModified(), DateTime.parse("2017-03-31T21:23:04Z"));
        assertNotNull(review.getUser());
    }

    @Test
    public void canParseUserJson(){
        JsonNode json = new JsonNode("{\n" +
                "\"_class\": \"user\",\n" +
                "\"id\": 30660398,\n" +
                "\"title\": \"Tintu Pathrose\",\n" +
                "\"name\": \"Tintu\",\n" +
                "\"display_name\": \"Tintu Pathrose\"\n" +
                "}");

        User user = alexRESTClient.jsonToUser(json.getObject());
        assertEquals(user.getDisplayName(), "Tintu Pathrose");
        assertEquals(user.getName(), "Tintu");
        assertEquals(user.getTitle(), "Tintu Pathrose");
    }

    @Test
    public void canParseCourseJson(){
        JsonNode json = new JsonNode("{\n" +
                "\"_class\": \"course\",\n" +
                "\"id\": 1075642,\n" +
                "\"title\": \"Apache Kafka Series - Learn Apache Kafka for Beginners\",\n" +
                "\"url\": \"/apache-kafka-series-kafka-from-beginner-to-intermediate/\"\n" +
                "}");

        Course course = alexRESTClient.jsonToCourse(json.getObject());
        assertEquals(course.getId(), (Long) 1075642L);
        assertEquals(course.getTitle(), "Apache Kafka Series - Learn Apache Kafka for Beginners");
        assertEquals(course.getUrl(), "/apache-kafka-series-kafka-from-beginner-to-intermediate/");
    }

    @Test
    public void twoApiCallsReturnTwoDifferentSetsOfResults() throws HttpException {
        List<Review> reviews1 = alexRESTClient.getNextReviews();
        List<Review> reviews2 = alexRESTClient.getNextReviews();
        // the first review of the new batch is after the last review of the old batch
        assertTrue(reviews2.get(0).getCreated().isAfter(reviews1.get(reviews1.size() - 1).getCreated()));
    }
	
	
}
