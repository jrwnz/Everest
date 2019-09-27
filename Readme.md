# From URLS to Users with Everest

This repository contains the second part of a project I completed for a client while a fellow at Insight Data Science.

The first part of the project entailed investigating the cause of a spike in links from other websites to the client's site, as measured by ahrefs.com. For commercial sensitivity reasons this part of the project is not public.

This, the second part of the project, was put together over the final week of the project period, and comprises tools allowing the client to investigate connections between sites linking to the client's site, specifically via HBDSCAN clustering over the text of each site (transformed to tf-idf space), and through http links using networkx. The Setup notebook allows the client to upload new ahrefs data then scrape the new sites (or re-scrape old ones), then perform the clustering algorithm. The Results notebook allows them to explore the results. The work all happens in the 'everest' module, and data and results are stored in a postgres database. Some code has been modified and/or withheld at the request of the client for commercial sensitivity reasons.