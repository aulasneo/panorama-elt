SELECT
 "org"
, "course_id"
, "user_id"
, "count_if"((role = 'staff')) "is_course_staff"
, "count_if"((role = 'instructor')) "is_course_instructor"
, "count_if"((role = 'course_creator_group')) "is_course_creator_group"
, "count_if"((role = 'beta_testers')) "is_course_beta_testers"
,  "lms"
FROM
  panorama.openedx_table_student_courseaccessrole
GROUP BY org, course_id, user_id, lms
