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

{% docs interview_feedback_delay %}
A metric that shows the delay between the actual completion of an interview and the moment the interviewer provides final feedback.
This helps businesses track the speed of decision-making: the lower the delay, the faster we move through the hiring funnel.
{% enddocs %}

{% docs interview_point_in_time_join %}
Special logic for joining tables by time range. Instead of simply taking the current candidate data, we look for a record where the interview creation date falls between the 'start' and 'end' dates of the candidate's data relevance.
{% enddocs %}
