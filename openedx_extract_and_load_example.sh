LMS_HOST=$(hostname)

export PANORAMA_RAW_DATA_BUCKET='panorama-raw-data-bucket'
export PANORAMA_AWS_ACCESS_KEY=
export PANORAMA_AWS_SECRET_ACCESS_KEY=
export PANORAMA_AWS_REGION=us-east-1

export PANORAMA_MYSQL_USERNAME=root
export PANORAMA_MYSQL_PASSWORD=
export PANORAMA_MYSQL_HOST=
export PANORAMA_MYSQL_DATABASE=edxapp

export PANORAMA_MYSQL_TABLES="auth_user,student_courseenrollment,auth_userprofile,student_courseaccessrole,course_overviews_courseoverview,courseware_studentmodule,grades_persistentcoursegrade,student_manualenrollmentaudit,student_courseenrollmentallowed,certificates_generatedcertificate"
export PANORAMA_BASE_PARTITIONS="{\"lms\":\"$LMS_HOST\"}"
export PANORAMA_TABLE_PARTITIONS='{"courseware_studentmodule": {"partition_fields": ["course_id"], "interval": "2 hour", "timestamp_field": "modified", "datalake_db": "panorama", "datalake_table": "courseware_studentmodule_raw", "workgroup": "panorama"}}'

export PANORAMA_MONGODB_USERNAME=edxapp
export PANORAMA_MONGODB_PASSWORD=
export PANORAMA_MONGODB_HOST=localhost
export PANORAMA_MONGODB_DATABASE=edxapp

venv/bin/python openedx_extract_and_load.py