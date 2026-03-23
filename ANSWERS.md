# Part 2 — Data Governance

## 1. How would you ensure business metrics are consistently defined across dashboards?

The main idea is to maintain logic consistency across dashboards. Otherwise we quickly lose structure and we spend more time fixing understanding issues from different users rather than actually getting work done. 

I would define key metrics in a shared layer, for example in Power BI datasets; SQL tables; or Databricks tables, and make sure dashboards reuse those instead of rewriting them.

I would also keep a simple document listing the metric definition, the owner, and how it is calculated. The best option is probably a Confluence page for this, frequently documented. 

Naming is important as well. Clear names like `active_users_7d` or `active_users_30days` avoid confusion.

Finally, for important metrics, I would add simple checks in the pipeline, for example comparing current values to recent values, to detect unexpected changes early.

## 2. How would you detect if a dashboard is using incorrect or outdated data sources?

If a dataset or table hasn’t been updated as expected, anything depending on it should be flagged.

The Power BI API allows us to see which dataset a report is using. We can compare this with a list of approved datasets and flag outdated ones.

If a column is removed or changed, it often breaks reports. Monitoring schema changes helps catch issues early.

Another thing to check is unexpected changes in key metrics. If a number suddenly changes a lot compared to recent values, it’s usually worth investigating.

## 3. If two teams define the same metric differently (e.g. Active Users), how would you resolve it?

First step is to understand why the definitions are different.

For example, “Active Users” could mean 7-day active users or 30-day active users. In that case, both definitions can exist, but they need clear names like active_users_7d and active_users_30d

If it is truly the same metric but defined differently, then the decision should be made with business stakeholders, not only by the data team.

Once a definition is agreed, it should be documented clearly (as I mentioned above, in a Confluence page or similar), dashboards should be updated, and the old version should be removed or marked as deprecated.

# Part 3 — Dashboard Design Thinking

All metrics are based on the `bi_assets` table created in in the pipeline.

1. Percentage % of reports with successful refresh  
Gives a quick overview of how healthy the reporting layer is.

2. Failed and never-refreshed reports by workspace  
Helps identify which teams are impacted and who should act.

3. Reports without an owner  
These are risky because no one is clearly responsible for fixing issues.

4. Refresh failures over time  
Useful to spot trends instead of reacting only to single failures.

5. Reports not refreshed within expected time  
Even if a report shows SUCCESS, it may still be outdated if it hasn’t refreshed recently.

6. Distribution of report status  
Giving an overview of SUCCESS, FAILED, NEVER_REFRESHED, NOT_REFRESHABLE, REFRESH_UNKNOWN. This would help understand the overall state of the system.

7. Pipeline health (last ingestion time)  
Ensures the pipeline itself is running. A missing run is a different issue than a failed refresh.

8. Dashboard usage drops  
Not yet available from the standard REST API, requires the Admin API's activity log (`getActivityEvents`). Once `views_last_30d` and `last_viewed` are populated, flag reports with a significant week-over-week drop in views. Useful for catching reports that have silently become irrelevant or broken in a way that doesn't show up as a refresh failure.