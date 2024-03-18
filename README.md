# eviction_notice_bot_taymnichols
This bot will scrape the eviction notices website and alert us when there is new data available

# Where I'm at currently
So far, I created a scraper to download the PDFs, use tabula to read the tables and convert them to csvs, stack all the csvs together and then remove duplicate rows. This process is currently a manual process and is not set up to run all the time. I need to somehow make it only do this if there is new data available and automate the process to run daily or weekly. 

I also served my csv on Datasette. Ultimately I would like this process to be automatic and to only run if there is a new pdf available. I also want to make it so it just grabs the new pdf and adds it to my csv so I don't lose historical data.

I also need to add a way to plot where these locations are on the map, and then tell me how many notices there are per ward in the new data. I also would like it to tell me if there are more than 5 new evictions scheduled at one specific base address, not including apartment numbers.