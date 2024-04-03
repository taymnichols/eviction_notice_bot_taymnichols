# eviction_notice_bot_taymnichols
This bot will scrape the eviction notices website and alert us when there is new data available

#### April 3 Update
Added the email set up using Mailgun - have not tested it out yet.

## March 30 Update
So far, I created a scraper to download the PDFs, used Tabula to read the tables and convert them to csvs, stack all the csvs together and then remove duplicate rows. 

Last week I automated this process to run every day at 6 pm and update the CSV if there is a new pdf available. I now have a functioning, automatic scraper which is really cool.

# What I need to do
* Add a way to plot where these locations are on the map

* get the dataset to route that to each ward and identify ward and hotspots

* set bot to send email including info on number of evictions per ward and eviction hotspots

* create a map

* automate the Datasette serve part of this process - since this part is in a shell file and the scraper is in a python file, I THINK I can just set my python code up IN the shell file and tell it to open python first? that way the whole process will be automatic. I'm not 100 percent sure about this though.

* Set up the bot part - Tell me when there is new data available, how many new rows there are, how many notices there are per ward in the new data and if there are more than 5 new evictions scheduled at one specific base address, not including apartment numbers.

* Simplify my scrape process - I had a lot of help from ChatGPT on this and I am not 100 percent clear on some of the things it did that deviate from what we used in class (for example, os.makedirs(csv_directory, exist_ok=True) is different from how we did this in class). It also inserted some code I think might be unnecessary - it took a really long time and a lot of tries to get it to finally filter out duplicate rows. I would like to go through and sub in what we did in class but I'm afraid to break it so will probably need some help with this.

# Future Goals
Ultimately, I want my bot to send me an email (I think) with the number of new eviction notices added, the number of notices per ward, and any eviction hotspots. I want to add a map to my database. Maybe it could send me a map in the email. Who knows.

# What have I learned about the data process so far?
* I have a good understanding of the data getting process sort of although I think mine was a little different because I am downloading PDFs and changing them into csvs which is basicall what we just did in class. I also don't feel like I understand the process as well as I would like because I think I over-relied on chatgpt
* I now feel pretty confident about setting up a scraper to run automatically. It took a fair amount of trial and error to get my github actions to run but I think I understand the process fairly well from that experience.
* I feel pretty comfortable in setting up this type of web scraper now - I had to change my code to get the hrefs ending in .pdf, check if there were tables in the pdf, pull the header row, and do a lot of "load if present, create if not" type things
* My data is pretty clean luckily, but there are a lot of duplicates. It also looks like an updated PDF was put on the site last week. I think unfortunately I may have deleted the old pdf when I reran the scraper manually and hadn't set it up to only get new pdfs, but luckily that won't be an issue going forward. 