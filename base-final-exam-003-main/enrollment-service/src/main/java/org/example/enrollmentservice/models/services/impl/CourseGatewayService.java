package org.example.enrollmentservice.models.services.impl;

import feign.FeignException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import org.example.enrollmentservice.clients.CourseClient;
import org.example.enrollmentservice.exceptions.CourseNotFoundException;
import org.example.enrollmentservice.exceptions.CourseServiceException;
import org.example.enrollmentservice.models.dto.responses.CourseResponse;
import org.springframework.util.StringUtils;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class CourseGatewayService {

    private final CourseClient courseClient;

    @CircuitBreaker(name = "courseService", fallbackMethod = "getCourseByIdFallback")
    public CourseResponse getCourseById(Long courseId) {
        CourseResponse course;
        try {
            course = courseClient.getCourseById(courseId);
        } catch (FeignException.NotFound exception) {
            throw new CourseNotFoundException(courseId);
        } catch (FeignException exception) {
            throw new CourseServiceException("Course Service request failed", exception);
        }

        if (course == null
                || course.courseId() == null
                || !courseId.equals(course.courseId())
                || !StringUtils.hasText(course.courseName())
                || course.courseFee() == null
                || course.courseFee() < 0) {
            throw new CourseServiceException("Course Service returned invalid course data for id: " + courseId);
        }

        return course;
    }

    public CourseResponse getCourseByIdFallback(Long courseId, Throwable exception) {
        if (exception instanceof CourseNotFoundException courseNotFoundException) {
            throw courseNotFoundException;
        }
        if (exception instanceof CourseServiceException courseServiceException) {
            throw courseServiceException;
        }
        if (exception instanceof FeignException feignException && feignException.status() == 404) {
            throw new CourseNotFoundException(courseId);
        }

        throw new CourseServiceException("Course Service is unavailable for course id: " + courseId, exception);
    }

}
