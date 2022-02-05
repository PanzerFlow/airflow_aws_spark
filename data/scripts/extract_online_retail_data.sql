COPY (
       select * from airflow.project.movieonlineretail -- we should have a date filter here to pull only required data
) TO '{{ params.user_purchase }}' WITH (FORMAT CSV, HEADER);