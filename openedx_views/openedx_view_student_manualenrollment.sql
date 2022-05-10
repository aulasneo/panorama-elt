SELECT
  "enrollment_id"
, count("id") as count_of_transitions
, "enrolled_email"
, "lms"
, "max_by"(state_transition, time_stamp) last_state_transition
, "max"(time_stamp) last_time_stamp
, "min"(time_stamp) first_time_stamp
, "min_by"(reason, time_stamp) first_reason
, array_join(array_agg(state_transition ORDER BY time_stamp ASC), ',', '') state_transitions
, array_join(array_agg(reason ORDER BY time_stamp ASC), ',', '') reasons
, array_join(array_agg(enrolled_by_id ORDER BY time_stamp ASC), ',', '') enrolled_by_ids
, array_join(array_agg(time_stamp ORDER BY time_stamp ASC), ',', '') transitions_time_stamps
FROM
  panorama.openedx_table_student_manualenrollmentaudit
GROUP BY enrollment_id, enrolled_email, lms
