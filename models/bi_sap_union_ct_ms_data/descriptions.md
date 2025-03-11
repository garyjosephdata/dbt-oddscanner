{% docs bi_sap_union_ct_ms_data_description %}

This table contains metrics (clicks, signups, FTDs, etc.) from the `sap_ct_ms_data` and `sap_dv_affiliate_links` tables.

**Process:**

1. Records from both tables were rearranged into the same schema.  
2. For each table, only records from advertisers that work with DV or MS respectively were retained.  
3. The results from the previous steps were then combined using a union.

{% enddocs %}