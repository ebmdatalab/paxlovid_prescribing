# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: all
#     notebook_metadata_filter: all,-language_info
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.3
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

import os
import pandas as pd
import numpy as np
from ebmdatalab import bq
import datetime
pd.set_option('display.max_rows', 10)

# ## Discrepancies between OpenSAFELY and OpenPrescribing prescribing of Paxlovid##

# A user reported a discrepancy between prescriptions for BNF code `0503060B0` (Nirmatrelvir with Ritonavir [Paxlovid]) on OpenPrescribing compared to OpenSAFELY.  OpenPrescribing shows [285 prescriptions in July 2023](https://openprescribing.net/chemical/0503060B0/), whereas OpenSafely shows only ~20 in the same month.  This number has increased to 720 items in OpenPrescribing in August 2023.

# ### Possible Reasons ###

# A number of reasons have been considered for this discrepancy, including codelist rot, mismapping of BNF to dm+d, and EHR vendor.
# However, a likely cause is that prescribing is being captured by OpenPrescribing from FP10 prescriptions which are not from GP practices.

# There are couple of ways we can check this, using Google BigQuery, which holds the raw data which OpenPrescribing uses:

# #### 1. Filtering to only TPP practices ####

# The [NHS Digital Patient Online Management Information (POMI)](https://digital.nhs.uk/data-and-information/publications/statistical/mi-patient-online-pomi/current) dataset has the current EHR vendor each GP practice uses.  By filtering to TPP (or other vendor), we can have a better match with OpenSAFELY records.

# +
sql = """
SELECT
  DATE(rx.month) AS month,
  pomi.system_supplier,
  SUM(items) AS items
FROM
  ebmdatalab.hscic.normalised_prescribing AS rx
LEFT OUTER JOIN
  richard.nhsd_system_supplier AS pomi
ON
  rx.practice = pomi.practice_code
  AND DATE(rx.month) = pomi.month
WHERE
  bnf_code LIKE '0503060B0%'
GROUP BY
  rx.month,
  pomi.system_supplier
"""

exportfile = os.path.join("..","data","pax_df.csv")
pax_df = bq.cached_read(sql, csv_path=exportfile, use_cache=False)
# -
paxpiv_df=pax_df.pivot(index='month', columns='system_supplier', values='items').replace(np.nan, 0)
pd.set_option('display.max_rows', 20)
display(paxpiv_df)

# For the above, it can be seen that the vast majority of prescriptions are written with practice codes which are not listed in the POMI dataset, and is therefore likely to be a non-GP setting.

# #### 2. Use setting data in NHSBSA data ####

# +
sql = """
SELECT
  DATE(rx.month) AS month,
  rx.practice,
  prac.name,
  place.code,
  place.setting,
  SUM(total_list_size) AS list_size,
  SUM(items) AS items
FROM
  `ebmdatalab.hscic.normalised_prescribing` AS rx
INNER JOIN
  hscic.practices AS prac
ON
  prac.code = rx.practice
INNER JOIN
  richard.prescription_setting AS place
ON
  prac.setting = place.code
LEFT OUTER JOIN
  hscic.practice_statistics AS stats
ON
  rx.practice = stats.practice
  AND rx.month = stats.month
WHERE
  bnf_code LIKE '0503060B0%'
GROUP BY
  rx.month,
  practice,
  name,
  place.code,
  place.setting
"""

exportfile = os.path.join("..","data","pax_set_df.csv")
pax_set_df = bq.cached_read(sql, csv_path=exportfile, use_cache=False)
# -

pax_set_piv_df = pd.pivot_table(pax_set_df, values='items', index=['month'],
                       columns=['setting'], aggfunc="sum", fill_value=0)
display(pax_set_piv_df)

# The majority of prescribing is occuring outside of the GP practice setting.  However we are getting different figures (e.g. 74 items) from this analysis than our previous methodology.  However, the practice setting can sometimes be unreliable.  We can check this by looking at the practice list sizes:

pax_set_gp_df = pax_set_df[(pax_set_df['code'] == 4) & (pax_set_df['month'] == '2023-07-01')].sort_values(by='items', ascending=False) 
pd.set_option('display.max_rows', None)
display(pax_set_gp_df)

# For July 2023, a large number of prescriptions prescribed against a "GP Practice" setting do not have a list size, and indeed the highest prescribed is called "NCL COVID MEDICINES DELIVERY UNIT", and has therefore been misclassified as a GP practice.

# ## Conclusion ##

# The discrepancy of prescribing of Nirmatrelvir with Ritonavir between OpenSAFELY and OpenPrescribing appears to be due to a high number of non-GP practice settings prescribing in primary care, and is therefore both systems are likely to be showing correct values.
