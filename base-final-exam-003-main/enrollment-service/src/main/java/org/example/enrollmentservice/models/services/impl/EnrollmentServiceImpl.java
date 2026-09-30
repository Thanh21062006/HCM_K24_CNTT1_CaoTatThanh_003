package org.example.enrollmentservice.models.services.impl;

import lombok.RequiredArgsConstructor;
import org.example.enrollmentservice.exceptions.CourseNotFoundException;
import org.example.enrollmentservice.models.constants.EnrollmentStatus;
import org.example.enrollmentservice.exceptions.DuplicateCourseException;
import org.example.enrollmentservice.models.dto.requests.CreateEnrollmentDetailRequest;
import org.example.enrollmentservice.models.dto.requests.CreateEnrollmentRequest;
import org.example.enrollmentservice.models.dto.responses.EnrollmentDetailResponse;
import org.example.enrollmentservice.models.dto.responses.EnrollmentResponse;
import org.example.enrollmentservice.models.dto.responses.CourseResponse;
import org.example.enrollmentservice.models.entities.Enrollment;
import org.example.enrollmentservice.models.entities.EnrollmentDetail;
import org.example.enrollmentservice.models.repositories.EnrollmentDetailRepository;
import org.example.enrollmentservice.models.repositories.EnrollmentRepository;
import org.example.enrollmentservice.models.services.EnrollmentService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
@RequiredArgsConstructor
public class EnrollmentServiceImpl implements EnrollmentService {

        private final EnrollmentRepository enrollmentRepository;
        private final EnrollmentDetailRepository enrollmentDetailRepository;
        private final CourseGatewayService courseGatewayService;
        private final KafkaTemplate<String, String> kafkaTemplate;

        @Value("${notification.kafka.enrollment-created-topic:enrollment-created}")
        private String enrollmentCreatedTopic;

        @Override
        @Transactional
        public EnrollmentResponse createEnrollment(CreateEnrollmentRequest request) {
                Set<Long> courseIds = new HashSet<>();
                List<CourseResponse> courses = new ArrayList<>();
                BigDecimal totalFee = BigDecimal.ZERO;

                for (CreateEnrollmentDetailRequest item : request.items()) {
                        if (!courseIds.add(item.courseId())) {
                                throw new DuplicateCourseException();
                        }

                        CourseResponse course = courseGatewayService.getCourseById(item.courseId());
                        courses.add(course);
                        totalFee = totalFee.add(BigDecimal.valueOf(course.courseFee()));
                }

                Enrollment enrollment = enrollmentRepository.save(Enrollment.builder()
                                .studentName(request.studentName())
                                .studentEmail(request.studentEmail())
                                .totalFee(totalFee.doubleValue())
                                .status(EnrollmentStatus.PENDING)
                                .build());

                List<EnrollmentDetail> details = courses.stream()
                                .map(course -> EnrollmentDetail.builder()
                                                .enrollment(enrollment)
                                                .courseId(course.courseId())
                                                .courseFee(course.courseFee())
                                                .build())
                                .toList();
                List<EnrollmentDetail> savedDetails = enrollmentDetailRepository.saveAll(details);

                List<EnrollmentDetailResponse> itemResponses = new ArrayList<>();
                for (int index = 0; index < savedDetails.size(); index++) {
                        EnrollmentDetail detail = savedDetails.get(index);
                        CourseResponse course = courses.get(index);
                        itemResponses.add(new EnrollmentDetailResponse(
                                        detail.getId(),
                                        detail.getCourseId(),
                                        course.courseName(),
                                        detail.getCourseFee(),
                                        detail.getCourseFee()));
                }

                if (TransactionSynchronizationManager.isSynchronizationActive()) {
                        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                                @Override
                                public void afterCommit() {
                                        kafkaTemplate.send(enrollmentCreatedTopic, request.studentEmail());
                                }
                        });
                } else {
                        kafkaTemplate.send(enrollmentCreatedTopic, request.studentEmail());
                }

                return new EnrollmentResponse(
                                enrollment.getId(),
                                enrollment.getStudentName(),
                                enrollment.getStudentEmail(),
                                enrollment.getTotalFee(),
                                enrollment.getStatus(),
                                itemResponses);
        }

}
