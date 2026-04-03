{% docs fct_interview %}
This fact table captures every interview event. It uses **Point-in-Time (SCD Type 2)** logic to join candidate attributes, ensuring that the report reflects the candidate's
status *at the exact time* the interview occurred, not their current status.
{% enddocs %}

{% docs interview_duration %}
Calculated as the difference in minutes between `started_datetime` and `feedback_provided_datetime`.
This metric is critical for assessing interviewer efficiency and process bottlenecks.
{% enddocs %}

{% docs candidate_staffing_status %}
The historical status of the candidate during the recruitment process (e.g., 'Available', 'Proposed').
Because we use historical joins, this field allows for accurate conversion rate analysis over time.
{% enddocs %}
