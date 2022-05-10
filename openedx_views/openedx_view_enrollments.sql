SELECT
  student_courseenrollment.user_id user_id
, auth_user.username username
, auth_userprofile.name user_fullname
, auth_user.email user_email
, auth_userprofile.country user_country
, country.latitude latitude
, country.longitude longitude
, student_courseenrollment.lms lms
, course_structures.organization organization
, course_structures.course_code course_code
, course_structures.course_edition course_edition
, course_structures.display_name course_name
, student_courseenrollment.course_id course_id
, course_overviews_courseoverview."start" course_start_date
, course_overviews_courseoverview."end" course_end_date
, student_courseenrollment.id enrollment_id
, IF((student_courseenrollment.created IS NULL), null, "date_diff"('Day', student_courseenrollment.created, "now"())) days_since_enrolled
, (student_courseenrollment.is_active = 1) enrollment_is_active
, student_courseenrollment.mode enrollment_mode
, student_courseenrollment.created date_enrolled
, IF((student_manualenrollmentaudit.last_state_transition LIKE '%to unenrolled'), student_manualenrollmentaudit.last_time_stamp) date_unenrolled
, "round"((grades_persistentcoursegrade.percent_grade * 100)) percent_grade
, grades_persistentcoursegrade.letter_grade letter_grade
, grades_persistentcoursegrade.modified date_graded
, (auth_user.is_active = 1) user_is_active
, (auth_user.is_staff = 1) user_is_global_staff
, (student_courseaccessrole.is_course_staff is not null and student_courseaccessrole.is_course_staff = 1) user_is_course_staff
, (student_courseaccessrole.is_course_instructor is not null and student_courseaccessrole.is_course_instructor = 1) user_is_course_instructor
, (student_courseaccessrole.is_course_beta_testers is not null and student_courseaccessrole.is_course_beta_testers = 1) user_is_course_beta_tester
, (student_courseaccessrole.is_course_creator_group is not null and student_courseaccessrole.is_course_creator_group = 1) user_is_course_creator_group
, student_courseaccessrole.is_course_staff is null or (student_courseaccessrole.is_course_staff = 0 AND student_courseaccessrole.is_course_instructor = 0 AND student_courseaccessrole.is_course_creator_group = 0 AND auth_user.is_staff = 0) is_student
, student_manualenrollmentaudit.last_state_transition last_state_transition
, student_manualenrollmentaudit.last_time_stamp last_state_transition_time_stamp
, student_manualenrollmentaudit.count_of_transitions count_of_transitions
, student_manualenrollmentaudit.transitions_time_stamps
, student_manualenrollmentaudit.state_transitions state_transitions
, student_manualenrollmentaudit.reasons reasons
, student_manualenrollmentaudit.enrolled_by_ids
FROM
  ((((((((panorama.openedx_table_student_courseenrollment student_courseenrollment
LEFT JOIN panorama.openedx_table_auth_user auth_user ON ((auth_user.id = student_courseenrollment.user_id) AND (auth_user.lms = student_courseenrollment.lms)))
LEFT JOIN panorama.openedx_table_auth_userprofile auth_userprofile ON ((auth_userprofile.user_id = student_courseenrollment.user_id) AND (auth_userprofile.lms = student_courseenrollment.lms)))
LEFT JOIN panorama.openedx_table_grades_persistentcoursegrade grades_persistentcoursegrade ON (((grades_persistentcoursegrade.user_id = student_courseenrollment.user_id) AND (grades_persistentcoursegrade.course_id = student_courseenrollment.course_id)) AND (grades_persistentcoursegrade.lms = student_courseenrollment.lms)))
LEFT JOIN panorama.openedx_table_course_overviews_courseoverview course_overviews_courseoverview ON ((course_overviews_courseoverview.id = student_courseenrollment.course_id) AND (course_overviews_courseoverview.lms = student_courseenrollment.lms)))
LEFT JOIN panorama.openedx_table_course_structures course_structures ON ((course_structures.module_location = student_courseenrollment.course_id) AND (course_structures.lms = student_courseenrollment.lms)))
LEFT JOIN panorama.country_codes_raw country ON (auth_userprofile.country = country.alpha2_code))
LEFT JOIN panorama.openedx_view_student_courseaccessrole student_courseaccessrole ON (((student_courseaccessrole.course_id = student_courseenrollment.course_id) AND (student_courseaccessrole.user_id = student_courseenrollment.user_id)) AND (student_courseaccessrole.lms = student_courseenrollment.lms)))
LEFT JOIN panorama.openedx_view_student_manualenrollmentaudit student_manualenrollmentaudit ON ((student_manualenrollmentaudit.enrollment_id = student_courseenrollment.id) AND (student_manualenrollmentaudit.lms = student_courseenrollment.lms)))
