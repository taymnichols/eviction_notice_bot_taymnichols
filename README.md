# eviction_notice_bot_taymnichols
This bot will scrape the eviction notices website and alert us when there is new data available

# Where I'm at currently
So far, I created a scraper to download the PDFs, use tabula to read the tables and convert them to csvs, stack all the csvs together and then remove duplicate rows. This process is currently a manual process and is not set up to run all the time. I need to somehow make it only do this if there is new data available and automate the process to run daily or weekly. 

I also served my csv on Datasette. Ultimately I would like this process to be automatic and to only run if there is a new pdf available. I also want to make it so it just grabs the new pdf and adds it to my csv so I don't lose historical data.

I also need to add a way to plot where these locations are on the map, and then tell me how many notices there are per ward in the new data. I also would like it to tell me if there are more than 5 new evictions scheduled at one specific base address, not including apartment numbers. Currently the way I set this up means there isn't really anything for the bot to alert me to because the process isn't set to run in the background. I think I need some help setting this to run on its own and only grab new pdfs.

I also think I need to go through and simplify my scrape process - I had a lot of help from ChatGPT on this and I am not 100 percent clear on some of the things it did that deviate from what we used in class (for example, os.makedirs(csv_directory, exist_ok=True) is different from how we did this in class). It also inserted some code I think might be unnecessary - it took a really long time and a lot of tries to get it to finally filter out duplicate rows. I would like to go through and sub in what we did in class but I'm afraid to break it so will probably need some help with this.
